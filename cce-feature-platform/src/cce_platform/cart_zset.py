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

本地PoC用_LocalStateStore（来自redis_state_machine）回退，无需Redis集群。

用法：
  from cce_platform.cart_zset import CartService, CartItem, ProductCodecart = CartService()
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
import os
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4

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


# Priority weights for ranked display (higher = shown first)
# Based on CCE campaign rules: high-margin products ranked higher
PRODUCT_PRIORITY: dict[ProductCode, float] = {
    ProductCode.PREMIUM_FINANCING:  10.0,
    ProductCode.INVESTMENT_LINKED:   9.0,
    ProductCode.INVESTMENT:          8.0,
    ProductCode.INSURANCE:           7.0,
    ProductCode.TRAVEL_INSURANCE:    6.0,
    ProductCode.SAVINGS:             5.0,
    ProductCode.CARD:                4.0,
}

# Quote validity (minutes) for products with time-limited pricing
DEFAULT_QUOTE_VALIDITY: dict[ProductCode, int] = {
    ProductCode.PREMIUM_FINANCING: 60,    # 1 hour — rate-sensitive
    ProductCode.INVESTMENT_LINKED: 30,    # 30 min — NAV changes daily
    ProductCode.INVESTMENT:        30,
    ProductCode.INSURANCE:         1440,  # 24 hours — stable pricing
    ProductCode.TRAVEL_INSURANCE:  120,
    ProductCode.SAVINGS:           0,     # 0 = no expiry
    ProductCode.CARD:              0,
}


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
        # Auto-set priority from catalogue
        if self.priority == 0.0:
            self.priority = PRODUCT_PRIORITY.get(self.product, 1.0)
        # Auto-set expiry if not provided
        if self.expiry_ts == 0.0:
            valid_minutes = DEFAULT_QUOTE_VALIDITY.get(self.product, 0)
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
# Local backend (reuses pattern from redis_state_machine._LocalStateStore)
# ---------------------------------------------------------------------------

class _LocalCartStore:
    """File-backed fallback — same pattern as _LocalStateStore."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def _load(self) -> dict[str, Any]:
        if not self._path.exists():
            return {}
        with self._path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    def _save(self, data: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(f"{self._path.suffix}.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
        tmp.replace(self._path)

    def zadd(self, key: str, score: float, member: str) -> None:
        data = self._load()
        zset: list[list] = data.get(key, [])
        # Remove existing member with same item_id (update semantics)
        try:
            item_id = json.loads(member).get("item_id")
            zset = [e for e in zset if json.loads(e[1]).get("item_id") != item_id]
        except Exception:
            zset = [e for e in zset if e[1] != member]
        zset.append([score, member])
        zset.sort(key=lambda e: e[0])
        data[key] = zset
        self._save(data)

    def zrem(self, key: str, item_id: str) -> int:
        """Remove by item_id (not raw member string)."""
        data = self._load()
        zset: list[list] = data.get(key, [])
        before = len(zset)
        zset = [e for e in zset if json.loads(e[1]).get("item_id") != item_id]
        data[key] = zset
        self._save(data)
        return before - len(zset)

    def zrange_all(self, key: str) -> list[tuple[str, float]]:
        data = self._load()
        return [(e[1], e[0]) for e in data.get(key, [])]

    def zcard(self, key: str) -> int:
        data = self._load()
        return len(data.get(key, []))

    def delete(self, key: str) -> None:
        data = self._load()
        data.pop(key, None)
        self._save(data)

    def zrangebyscore(self, key: str, min_score: float, max_score: float) -> list[tuple[str, float]]:
        """Return members with score in [min_score, max_score]."""
        return [
            (member, score)
            for member, score in self.zrange_all(key)
            if min_score <= score <= max_score
        ]

    def close(self) -> None:
        pass


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
        local_store_path: Path | None = None,) -> None:
        url = redis_url or os.getenv("REDIS_URL")
        if url:
            try:
                self._backend, self._mode = self._make_redis(url), "redis"
                logger.info("CartService: Redis backend at %s", url)
            except Exception as exc:
                logger.warning("CartService: Redis unavailable (%s), using local store", exc)
                self._backend = self._make_local(local_store_path)
                self._mode = "local"
        else:
            self._backend = self._make_local(local_store_path)
            self._mode = "local"

    @staticmethod
    def _make_redis(url: str):
        try:
            import redis  # type: ignore[import]
        except ImportError as exc:
            raise ImportError("pip install redis>=5.0") from exc
        client = redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=3)
        client.ping()
        return client

    @staticmethod
    def _make_local(path: Path | None) -> _LocalCartStore:
        from .config import settings
        default = settings.base_dir / "data" / "online" / "cart_store.json"
        return _LocalCartStore(path or default)

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
        if self._mode == "redis":
            self._backend.zadd(key, {member: score})
        else:
            self._backend.zadd(key, score, member)

    def _zrange_all(self, key: str) -> list[tuple[str, float]]:
        if self._mode == "redis":
            return self._backend.zrange(key, 0, -1, withscores=True)
        return self._backend.zrange_all(key)

    def _zrevrange_all(self, key: str) -> list[tuple[str, float]]:
        """Return all members sorted by score descending."""
        entries = self._zrange_all(key)
        return sorted(entries, key=lambda e: e[1], reverse=True)

    def _zrem(self, key: str, item_id: str) -> None:
        if self._mode == "redis":
            # In Redis mode: fetch member, then ZREM
            for member, _ in self._zrange_all(key):
                try:
                    if json.loads(member).get("item_id") == item_id:
                        self._backend.zrem(key, member)
                        break
                except Exception:
                    pass
        else:
            self._backend.zrem(key, item_id)

    def _delete(self, key: str) -> None:
        if self._mode == "redis":
            self._backend.delete(key)
        else:
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

        if self._mode == "redis":
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
        if self._mode == "redis":
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
            "event_ts":     __import__("datetime").datetime.utcnow().isoformat(timespec="seconds"),
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
