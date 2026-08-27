"""
Cart ZSET — Redis-backed shopping cart for CCE financial products.

为什么用ZSET做购物车（而不是Hash）？- Hash适合"无序商品列表"（普通电商）
  - ZSET适合"有优先级/时序的金融产品篮子"：score = 加入时间戳 → 按加入顺序展示
      score = priority权重 → 按产品推荐优先级排序
      score = expiry时间戳 → 自动感知报价过期（保险/理财限时优惠）

CCE适配的金融场景：
  1. 产品篮子（Product Basket）：客户将INSURANCE/INVESTMENT/SAVINGS加入篮子
  2. 优先级排序：高价值产品（PREMIUM_FINANCING）优先展示
  3. 报价过期（Quote Expiry）：理财产品报价有效期30分钟，ZSET score存expiry_ts
  4. 跨设备同步：同一unified_customer_key在手机和PC上看到相同篮子
  5. 遗弃篮子分析：结合flink_cdc_pipeline的rt_cart_value_1d特征，触发挽回营销

Key设计：
  cart:{unified_customer_key}          → ZSET，member=product_item_json，score=add_ts
  cart:priority:{unified_customer_key} → ZSET，member=product_code，score=priority_weight
  cart:expiry:{unified_customer_key}   → ZSET，member=product_item_id，score=expiry_ts

本地PoC用 kv_backend.LocalZSetStore 回退（与状态机共用），无需Redis集群。

产品优先级与报价有效期属于业务策略，由 policy.py 从配置加载（见
config/business_policy.json），改数字不需要改代码、发版本。

Replica constraint:
  The local fallback is per-process, so carts are NOT shared between pods.
  Unlike the Feature API — which serves derived read-only data that every pod
  rebuilds identically from the deterministic gold pipeline — a cart is
  authoritative mutable state. Serving it from more than one replica on the
  local backend would give each pod its own basket, so a customer would see a
  different cart depending on which pod handled the request, breaking the
  cross-device sync promise above. This module is not wired into api.py today.
  Before exposing it over HTTP in a multi-replica deployment, provision a real
  Redis and leave CCE_REQUIRE_REDIS unset so staging and production fail fast
  instead of degrading (see config.py).

用法：
  from cce_platform.L2_olap.cart_zset import CartService, CartItem, ProductCode

  cart = CartService()
  cart.add_item("U0001", CartItem(product=ProductCode.INSURANCE, amount=1380.0, quote_valid_minutes=30))
  cart.add_item("U0001", CartItem(product=ProductCode.INVESTMENT, amount=2100.0))
  items = cart.get_items("U0001")          # 按加入时间排序
  ranked = cart.get_ranked_items("U0001")  # 按优先级排序
  cart.remove_item("U0001", item_id)
  cart.clear("U0001")
  summary = cart.get_summary("U0001")      # 含过期检测
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, UTC
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..L1_mechanism import LocalZSetStore, REDIS_MODE, make_kv_backend
from ..L1_business_data import product_priority, quote_validity_minutes

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Product catalogue (mirrors CCE pipeline.py product types)
# ---------------------------------------------------------------------------

class ProductCode(str, Enum):
    INSURANCE          = "INSURANCE"
    PREMIUM_FINANCING  = "PREMIUM_FINANCING"
    INVESTMENT         = "INVESTMENT"
    INVESTMENT_LINKED  = "INVESTMENT_LINKED"
    SAVINGS            = "SAVINGS"
    TRAVEL_INSURANCE   = "TRAVEL_INSURANCE"
    CARD= "CARD"


# Display ranking and quote validity are business policy (campaign-owned), not
# infrastructure — loaded from config via policy.py rather than hardcoded here.
# See product_priority() and quote_validity_minutes().


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class CartItem:
    product:             ProductCode
    amount:              float
    currency:            str = "SGD"
    item_id:             str = field(default_factory=lambda: str(uuid4()))
    add_ts:              float = field(default_factory=time.time)
    expiry_ts:           float = 0.0      # 0 = no expiry
    priority:            float = 0.0      # filled on add
    metadata:            dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Priority and quote validity come from business policy (policy.py), so
        # a campaign change takes effect without a code release.
        if self.priority == 0.0:
            self.priority = product_priority(self.product.value)
        if self.expiry_ts == 0.0:
            valid_minutes = quote_validity_minutes(self.product.value)
            if valid_minutes > 0:
                self.expiry_ts = self.add_ts + valid_minutes * 60

    def is_expired(self, now: float | None = None) -> bool:
        if self.expiry_ts == 0.0:
            return False
        return (now or time.time()) > self.expiry_ts

    def to_member(self) -> str:
        """Serialize to ZSET member string (compact JSON, sorted keys)."""
        return json.dumps({
            "item_id":   self.item_id,
            "product":   self.product.value,
            "amount":    self.amount,
            "currency":  self.currency,
            "expiry_ts": self.expiry_ts,
            "priority":  self.priority,
            "metadata":  self.metadata,
        }, sort_keys=True)

    @classmethod
    def from_member(cls, member: str, score: float) -> "CartItem":
        d = json.loads(member)
        return cls(
            product=ProductCode(d["product"]),
            amount=d["amount"],
            currency=d.get("currency", "SGD"),
            item_id=d["item_id"],
            add_ts=score,
            expiry_ts=d.get("expiry_ts", 0.0),
            priority=d.get("priority", 1.0),
            metadata=d.get("metadata", {}),
        )


@dataclass
class CartSummary:
    customer_key:       str
    item_count:         int
    total_amount:       float
    currency:           str
    expired_count:      int
    items:              list[CartItem]
    has_high_value:     bool   # any item >= 1000 SGD → flag for compliance check
    recommended_action: str    # "checkout" | "review_expired" | "empty"


# ---------------------------------------------------------------------------
# Local backend
# ---------------------------------------------------------------------------

def _cart_member_identity(member: str) -> str:
    """Two cart members are the same entry when their item_id matches.

    This is the one behaviour that differs from the state machine's store, which
    compares whole member strings. Passing it to LocalZSetStore keeps re-adding
    an item as an in-place update without duplicating the store class.
    """
    return json.loads(member)["item_id"]


# ---------------------------------------------------------------------------
# Main CartService
# ---------------------------------------------------------------------------

class CartService:
    """ZSET-backed financial product cart for CCE.

    Three ZSETs per customer:
      cart:items:{key}     score=add_ts      → chronological order (default view)
      cart:priority:{key}  score=priority    → ranked by product weight (RM view)
      cart:expiry:{key}    score=expiry_ts   → expiry-sorted (cleanup / alert)

    Redis backend: set REDIS_URL env var.
    Local backend: used automatically when Redis is unavailable.
    """

    def __init__(
        self,
        redis_url: str | None = None,
        local_store_path: Path | None = None,
    ) -> None:
        # The local backend is per-process: degrading would give each replica its
        # own cart, so a customer's basket would change depending on which pod
        # served the request. make_kv_backend raises instead wherever the
        # environment requires Redis.
        self._backend, self._mode = make_kv_backend(
            "CartService",
            local_factory=lambda: self._make_local(local_store_path),
            redis_url=redis_url,
        )

    @staticmethod
    def _make_local(path: Path | None) -> LocalZSetStore:
        from ..L0_configuration import settings
        default = settings.base_dir / "data" / "online" / "cart_store.json"
        return LocalZSetStore(path or default, member_identity=_cart_member_identity)

    # -- Key helpers ---------------------------------------------------------

    @staticmethod
    def _items_key(customer_key: str) -> str:
        return f"cart:items:{customer_key}"

    @staticmethod
    def _priority_key(customer_key: str) -> str:
        return f"cart:priority:{customer_key}"

    @staticmethod
    def _expiry_key(customer_key: str) -> str:
        return f"cart:expiry:{customer_key}"

    # -- Internal ZSET ops ---------------------------------------------------

    def _zadd(self, key: str, score: float, member: str) -> None:
        if self._mode == REDIS_MODE:
            self._backend.zadd(key, {member: score})
        else:
            self._backend.zadd(key, score, member)

    def _zrange_all(self, key: str) -> list[tuple[str, float]]:
        if self._mode == REDIS_MODE:
            return self._backend.zrange(key, 0, -1, withscores=True)
        return self._backend.zrange_all(key)

    def _zrevrange_all(self, key: str) -> list[tuple[str, float]]:
        """Return all members sorted by score descending."""
        entries = self._zrange_all(key)
        return sorted(entries, key=lambda e: e[1], reverse=True)

    def _zrem(self, key: str, item_id: str) -> None:
        if self._mode == REDIS_MODE:
            # Redis ZREM matches the whole member, so find it by item_id first.
            for member, _ in self._zrange_all(key):
                try:
                    if json.loads(member).get("item_id") == item_id:
                        self._backend.zrem(key, member)
                        break
                except Exception:
                    pass
        else:
            self._backend.zrem_by_identity(key, item_id)

    def _delete(self, key: str) -> None:
        self._backend.delete(key)

    # -- Public API ----------------------------------------------------------

    def add_item(self, customer_key: str, item: CartItem) -> CartItem:
        """
        Add a product to the customer's cart.
        Idempotent by item_id: re-adding the same item_id updates it in place.

        Writes to three ZSETs atomically (best-effort; Redis pipeline used in prod):
          - cart:items:{key}    score=add_ts     (chronological)
          - cart:priority:{key} score=priority   (ranked display)
          - cart:expiry:{key}   score=expiry_ts  (only if item has expiry)
        """
        member = item.to_member()

        if self._mode == REDIS_MODE:
            # Use pipeline for multi-key atomicity
            with self._backend.pipeline() as pipe:
                pipe.zadd(self._items_key(customer_key),    {member: item.add_ts})
                pipe.zadd(self._priority_key(customer_key), {member: item.priority})
                if item.expiry_ts > 0:
                    pipe.zadd(self._expiry_key(customer_key), {member: item.expiry_ts})
                pipe.execute()
        else:
            self._backend.zadd(self._items_key(customer_key),    item.add_ts,   member)
            self._backend.zadd(self._priority_key(customer_key), item.priority, member)
            if item.expiry_ts > 0:
                self._backend.zadd(self._expiry_key(customer_key), item.expiry_ts, member)

        logger.info(
            "cart.add_item: customer=%s product=%s amount=%.2f %s expiry=%s",
            customer_key, item.product.value, item.amount, item.currency,
            "none" if item.expiry_ts == 0 else f"in {int((item.expiry_ts - time.time()) / 60)}min",
        )
        return item

    def get_items(self, customer_key: str, exclude_expired: bool = False) -> list[CartItem]:
        """Return items in chronological order (oldest first)."""
        entries = self._zrange_all(self._items_key(customer_key))
        items = [CartItem.from_member(m, s) for m, s in entries]
        if exclude_expired:
            now = time.time()
            items = [i for i in items if not i.is_expired(now)]
        return items

    def get_ranked_items(self, customer_key: str, exclude_expired: bool = True) -> list[CartItem]:
        """Return items sorted by product priority (highest first) — RM/advisor view."""
        entries = self._zrevrange_all(self._priority_key(customer_key))
        items = [CartItem.from_member(m, s) for m, s in entries]
        if exclude_expired:
            now = time.time()
            items = [i for i in items if not i.is_expired(now)]
        return items

    def get_expiring_soon(self, customer_key: str, within_minutes: int = 15) -> list[CartItem]:
        """
        Return items whose quote expires within `within_minutes`.
        Used by the notification service to trigger "Your quote is expiring soon" alerts.
        """
        now = time.time()
        deadline = now + within_minutes * 60
        if self._mode == REDIS_MODE:
            entries = self._backend.zrangebyscore(
                self._expiry_key(customer_key), now, deadline, withscores=True
            )
        else:
            entries = self._backend.zrangebyscore(
                self._expiry_key(customer_key), now, deadline
            )
        return [CartItem.from_member(m, s) for m, s in entries]

    def remove_item(self, customer_key: str, item_id: str) -> bool:
        """Remove a specific item by item_id from all three ZSETs."""
        self._zrem(self._items_key(customer_key),    item_id)
        self._zrem(self._priority_key(customer_key), item_id)
        self._zrem(self._expiry_key(customer_key),   item_id)
        logger.info("cart.remove_item: customer=%s item_id=%s", customer_key, item_id)
        return True

    def clear(self, customer_key: str) -> None:
        """Empty the entire cart (e.g. after successful checkout)."""
        self._delete(self._items_key(customer_key))
        self._delete(self._priority_key(customer_key))
        self._delete(self._expiry_key(customer_key))
        logger.info("cart.clear: customer=%s", customer_key)

    def purge_expired(self, customer_key: str) -> int:
        """
        Remove expired items from the cart.
        Call periodically or on cart load to keep cart clean.
        Returns count of removed items.
        """
        now = time.time()
        items = self.get_items(customer_key)
        expired = [i for i in items if i.is_expired(now)]
        for item in expired:
            self.remove_item(customer_key, item.item_id)
        if expired:
            logger.info(
                "cart.purge_expired: customer=%s removed %d expired items",
                customer_key, len(expired)
            )
        return len(expired)

    def get_summary(self, customer_key: str) -> CartSummary:
        """
        Full cart summary with expiry awareness and compliance flag.
        Used by the API layer and the RM dashboard.
        """
        all_items = self.get_items(customer_key)
        now = time.time()
        expired_count = sum(1 for i in all_items if i.is_expired(now))
        active_items = [i for i in all_items if not i.is_expired(now)]
        total_amount = round(sum(i.amount for i in active_items), 2)
        has_high_value = any(i.amount >= 1000.0 for i in active_items)

        if not active_items:
            action = "empty"
        elif expired_count > 0:
            action = "review_expired"
        else:
            action = "checkout"

        return CartSummary(
            customer_key=customer_key,
            item_count=len(active_items),
            total_amount=total_amount,
            currency="SGD",
            expired_count=expired_count,
            items=active_items,
            has_high_value=has_high_value,
            recommended_action=action,
        )

    def merge_anonymous_cart(
        self, anonymous_key: str, customer_key: str
    ) -> int:
        """
        Merge a pre-login anonymous cart into an authenticated customer cart.
        Used when a guest user logs in — common in DBS digibank flow.
        Items are added to the customer cart; anonymous cart is cleared.
        Returns number of items merged.
        """
        anon_items = self.get_items(anonymous_key, exclude_expired=True)
        existing_products = {i.product for i in self.get_items(customer_key)}
        merged = 0
        for item in anon_items:
            if item.product not in existing_products:
                # Reset add_ts to now so merged items sort after existing ones
                item.add_ts = time.time()
                self.add_item(customer_key, item)
                merged += 1
        if merged:
            self.clear(anonymous_key)
            logger.info(
                "cart.merge: %d items merged from %s → %s",
                merged, anonymous_key, customer_key
            )
        return merged

    def snapshot_to_cdc_event(self, customer_key: str) -> dict[str, Any]:
        """
        Export current cart as a CDC-style event for flink_cdc_pipeline ingestion.
        Allows cart abandonment signals to feed into rt_cart_value_1d feature.
        """
        summary = self.get_summary(customer_key)
        return {
            "table":        "cart_events",
            "op":           "snapshot",
            # Offset-aware, matching oltp.outbox. The naive utcnow() this
            # replaced serialized without an offset, so the consumer's
            # fromisoformat().timestamp() read it back as local time.
            "event_ts":     datetime.now(UTC).isoformat(timespec="seconds"),
            "after": {
                "cart_id":           f"SNAP-{customer_key}-{int(time.time())}",
                "unified_customer_key": customer_key,
                "id_type":           "UNIFIED",
                "id_value":          customer_key,
                "amount":            summary.total_amount,
                "product":           summary.items[0].product.value if summary.items else "UNKNOWN",
                "item_count":        summary.item_count,
                "has_high_value":    summary.has_high_value,
            },
        }

    def close(self) -> None:
        self._backend.close()

    def __enter__(self) -> "CartService":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
