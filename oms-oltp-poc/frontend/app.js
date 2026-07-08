const state = {
  customers: [],
  inventory: [],
  orders: [],
  outbox: [],
  summary: {},
};

const el = {
  metrics: document.querySelector("#metrics"),
  customerSelect: document.querySelector("#customerSelect"),
  skuSelect: document.querySelector("#skuSelect"),
  quantityInput: document.querySelector("#quantityInput"),
  inventoryRows: document.querySelector("#inventoryRows"),
  orderList: document.querySelector("#orderList"),
  eventList: document.querySelector("#eventList"),
  orderForm: document.querySelector("#orderForm"),
  message: document.querySelector("#message"),
  resetBtn: document.querySelector("#resetBtn"),
  publishBtn: document.querySelector("#publishBtn"),
};

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: response.statusText }));
    const message = error.detail?.message || error.detail || response.statusText;
    throw new Error(message);
  }
  return response.json();
}

function currency(cents) {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format((cents || 0) / 100);
}

function statusClass(status) {
  return String(status || "").toLowerCase();
}

function renderMetrics() {
  const orders = state.summary.orders || {};
  const outbox = state.summary.outbox || {};
  const metrics = [
    ["Reserved", orders.RESERVED || 0],
    ["Confirmed", orders.CONFIRMED || 0],
    ["Shipped", orders.SHIPPED || 0],
    ["Available Stock", state.summary.available_stock || 0],
    ["Reserved Stock", state.summary.reserved_stock || 0],
    ["Pending Events", outbox.PENDING || 0],
  ];
  el.metrics.innerHTML = metrics
    .map(([label, value]) => `<article class="metric"><span>${label}</span><strong>${value}</strong></article>`)
    .join("");
}

function renderCustomers() {
  const current = el.customerSelect.value;
  el.customerSelect.innerHTML = state.customers
    .map((customer) => `<option value="${customer.customer_id}">${customer.customer_id} - ${customer.customer_name}</option>`)
    .join("");
  if (current) el.customerSelect.value = current;
}

function renderSkuSelect() {
  const current = el.skuSelect.value;
  el.skuSelect.innerHTML = state.inventory
    .map(
      (sku) =>
        `<option value="${sku.sku_id}">${sku.sku_id} - ${sku.product_name} (${sku.available_stock} available)</option>`,
    )
    .join("");
  if (current) el.skuSelect.value = current;
}

function renderInventory() {
  el.inventoryRows.innerHTML = state.inventory
    .map(
      (row) => `
        <tr>
          <td class="id">${row.sku_id}</td>
          <td>${row.product_name}</td>
          <td>${row.available_stock}</td>
          <td>${row.reserved_stock}</td>
          <td>${row.sold_stock}</td>
        </tr>
      `,
    )
    .join("");
}

function renderOrders() {
  if (!state.orders.length) {
    el.orderList.innerHTML = `<p class="muted">No orders yet.</p>`;
    return;
  }
  el.orderList.innerHTML = state.orders
    .map((order) => {
      const items = order.items
        .map((item) => `${item.sku_id} x ${item.quantity}`)
        .join(", ");
      const canPay = order.status === "RESERVED";
      const canCancel = order.status === "RESERVED";
      const canShip = order.status === "CONFIRMED";
      return `
        <article class="order-row">
          <div class="order-head">
            <div>
              <div class="id">${order.order_id}</div>
              <div class="muted">${order.customer_name} - ${currency(order.total_amount_cents)}</div>
            </div>
            <span class="status ${statusClass(order.status)}">${order.status}</span>
          </div>
          <div class="items">${items}</div>
          <div class="row-actions">
            <button data-action="pay" data-order="${order.order_id}" ${canPay ? "" : "disabled"}>Payment Success</button>
            <button class="danger" data-action="fail" data-order="${order.order_id}" ${canPay ? "" : "disabled"}>Payment Fail</button>
            <button class="secondary" data-action="cancel" data-order="${order.order_id}" ${canCancel ? "" : "disabled"}>Cancel</button>
            <button class="secondary" data-action="ship" data-order="${order.order_id}" ${canShip ? "" : "disabled"}>Ship</button>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderOutbox() {
  if (!state.outbox.length) {
    el.eventList.innerHTML = `<p class="muted">No events yet.</p>`;
    return;
  }
  el.eventList.innerHTML = state.outbox
    .slice(0, 12)
    .map(
      (event) => `
        <article class="event-row">
          <div>
            <div class="event-type">${event.event_type}</div>
            <div class="id muted">${event.aggregate_type}:${event.aggregate_id}</div>
          </div>
          <span class="status ${statusClass(event.status)}">${event.status}</span>
        </article>
      `,
    )
    .join("");
}

function render() {
  renderMetrics();
  renderCustomers();
  renderSkuSelect();
  renderInventory();
  renderOrders();
  renderOutbox();
}

async function refresh() {
  const [summary, customers, inventory, orders, outbox] = await Promise.all([
    request("/api/summary"),
    request("/api/customers"),
    request("/api/inventory"),
    request("/api/orders"),
    request("/api/outbox"),
  ]);
  Object.assign(state, { summary, customers, inventory, orders, outbox });
  render();
}

async function withMessage(message, work) {
  el.message.textContent = message;
  try {
    await work();
    await refresh();
    el.message.textContent = "Done.";
  } catch (error) {
    el.message.textContent = error.message;
  }
}

el.orderForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await withMessage("Reserving inventory...", async () => {
    await request("/api/orders", {
      method: "POST",
      body: JSON.stringify({
        customer_id: el.customerSelect.value,
        idempotency_key: `ui-${Date.now()}`,
        items: [{ sku_id: el.skuSelect.value, quantity: Number(el.quantityInput.value) }],
      }),
    });
  });
});

el.orderList.addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button || button.disabled) return;
  const orderId = button.dataset.order;
  const action = button.dataset.action;
  await withMessage(`Running ${action}...`, async () => {
    if (action === "pay") {
      await request(`/api/orders/${orderId}/payment`, {
        method: "POST",
        body: JSON.stringify({ provider_ref: `ui-pay-${Date.now()}`, succeed: true }),
      });
    }
    if (action === "fail") {
      await request(`/api/orders/${orderId}/payment`, {
        method: "POST",
        body: JSON.stringify({ provider_ref: `ui-fail-${Date.now()}`, succeed: false }),
      });
    }
    if (action === "cancel") {
      await request(`/api/orders/${orderId}/cancel`, {
        method: "POST",
        body: JSON.stringify({ reason: "operator cancelled" }),
      });
    }
    if (action === "ship") {
      await request(`/api/orders/${orderId}/ship`, { method: "POST" });
    }
  });
});

el.resetBtn.addEventListener("click", async () => {
  await withMessage("Resetting demo data...", async () => {
    await request("/api/demo/reset", { method: "POST" });
  });
});

el.publishBtn.addEventListener("click", async () => {
  await withMessage("Publishing outbox...", async () => {
    await request("/api/outbox/publish", { method: "POST" });
  });
});

refresh().catch((error) => {
  el.message.textContent = error.message;
});
