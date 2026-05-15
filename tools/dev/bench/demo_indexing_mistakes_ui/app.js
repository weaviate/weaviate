// Demo UI for the "Common Indexing Mistakes" build.
//
// Runs pre-canned GraphQL queries against a Weaviate REST endpoint and renders
// the resulting wall-clock latency, top results, and (where applicable) total
// match counts. No client-side caching - every press of "Run query" hits the
// server fresh.

const LS_URL_KEY = "imd-url";
const LS_KEY_KEY = "imd-apikey";
const COLLECTION = "IndexingMistakesDemo";

const urlInput = document.getElementById("conn-url");
const keyInput = document.getElementById("conn-key");
const connLight = document.getElementById("conn-light");
const connStatusText = document.getElementById("conn-status-text");

// -- Persistence -------------------------------------------------------------

function loadConn() {
  urlInput.value = localStorage.getItem(LS_URL_KEY) || "http://localhost:8080";
  keyInput.value = localStorage.getItem(LS_KEY_KEY) || "";
}
function saveConn() {
  localStorage.setItem(LS_URL_KEY, urlInput.value.trim());
  localStorage.setItem(LS_KEY_KEY, keyInput.value.trim());
}
urlInput.addEventListener("change", saveConn);
keyInput.addEventListener("change", saveConn);
loadConn();

// -- Connection helpers ------------------------------------------------------

function baseUrl() {
  return (urlInput.value || "http://localhost:8080").replace(/\/+$/, "");
}
function isLocal(u) {
  try {
    const parsed = new URL(u);
    return parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1";
  } catch (_) {
    return false;
  }
}
function authHeaders() {
  const headers = { "content-type": "application/json" };
  const key = keyInput.value.trim();
  if (key && !isLocal(baseUrl())) {
    headers["authorization"] = `Bearer ${key}`;
  }
  return headers;
}

async function pingServer() {
  const url = baseUrl();
  try {
    const r = await fetch(`${url}/v1/meta`, { headers: authHeaders(), cache: "no-store" });
    if (!r.ok) throw new Error(`status ${r.status}`);
    const meta = await r.json();
    connLight.className = "conn-light ok";
    connStatusText.textContent = `connected (v${meta.version || "?"})`;
    return true;
  } catch (e) {
    connLight.className = "conn-light bad";
    connStatusText.textContent = `unreachable (${e.message})`;
    return false;
  }
}
document.getElementById("conn-test").addEventListener("click", () => pingServer());
pingServer();

// -- GraphQL transport -------------------------------------------------------

async function gqlRaw(query) {
  const r = await fetch(`${baseUrl()}/v1/graphql`, {
    method: "POST",
    headers: authHeaders(),
    cache: "no-store",
    body: JSON.stringify({ query }),
  });
  const text = await r.text();
  let body;
  try { body = JSON.parse(text); } catch (_) { body = { raw: text }; }
  if (!r.ok) {
    const err = new Error(`HTTP ${r.status}`);
    err.body = body;
    throw err;
  }
  if (body.errors && body.errors.length) {
    const err = new Error(body.errors.map(e => e.message).join("; "));
    err.body = body;
    throw err;
  }
  return body;
}

async function timedGql(query) {
  const t0 = performance.now();
  const body = await gqlRaw(query);
  const t1 = performance.now();
  return { body, ms: t1 - t0 };
}

// -- Query templates ---------------------------------------------------------

function qPriceRange() {
  return `{
    Get {
      ${COLLECTION}(
        where: {
          operator: And,
          operands: [
            { path: ["price_cents"], operator: GreaterThanEqual, valueInt: 30000 },
            { path: ["price_cents"], operator: LessThanEqual,    valueInt: 40000 }
          ]
        },
        limit: 10
      ) {
        name brand category price_cents sku
      }
    }
  }`;
}
function qPriceRangeCount() {
  return `{
    Aggregate {
      ${COLLECTION}(
        where: {
          operator: And,
          operands: [
            { path: ["price_cents"], operator: GreaterThanEqual, valueInt: 30000 },
            { path: ["price_cents"], operator: LessThanEqual,    valueInt: 40000 }
          ]
        }
      ) { meta { count } }
    }
  }`;
}

function qPathBm25() {
  return `{
    Get {
      ${COLLECTION}(
        bm25: { query: "/products/cameras/gopro", properties: ["spec_sheet_path"] },
        limit: 10
      ) {
        spec_sheet_path category brand name
      }
    }
  }`;
}

function qSkuBm25() {
  return `{
    Get {
      ${COLLECTION}(
        bm25: { query: "GP-ACT-BLK-001", properties: ["sku"] },
        limit: 10
      ) {
        sku brand category name
      }
    }
  }`;
}

function qCategoryEqual() {
  return `{
    Get {
      ${COLLECTION}(
        where: { path: ["category"], operator: Equal, valueText: "Cameras > Action" },
        limit: 10
      ) {
        name brand category sku price_cents
      }
    }
  }`;
}
function qCategoryEqualCount() {
  return `{
    Aggregate {
      ${COLLECTION}(
        where: { path: ["category"], operator: Equal, valueText: "Cameras > Action" }
      ) { meta { count } }
    }
  }`;
}

function qEmailBm25() {
  // Use a prefix that doesn't exist in the data so the only thing BM25 can
  // match on is the domain tokens. The top results will be `support@nikon.com`
  // even though we asked for `info@nikon.com` - that's the false positive.
  return `{
    Get {
      ${COLLECTION}(
        bm25: { query: "info@nikon.com", properties: ["support_email"] },
        limit: 10
      ) {
        support_email brand category name
      }
    }
  }`;
}

// -- Rendering helpers -------------------------------------------------------

function latencyClass(ms) {
  if (ms >= 1000) return "very-slow";
  if (ms >= 250) return "slow";
  return "";
}

function fmtMs(ms) {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  return `${ms.toFixed(0)} ms`;
}

function fmtPrice(cents) {
  if (cents == null) return "";
  return `$${(cents / 100).toFixed(2)}`;
}

function renderError(container, err) {
  const msg = err.message || String(err);
  // Recognise the specific "requires inverted index" error - that IS the
  // failure mode we're demonstrating for cards 1 and 4, so frame it as a
  // demo outcome rather than an unexpected error.
  const requiresIndex = /requires inverted index/i.test(msg);
  if (requiresIndex) {
    container.innerHTML = `
      <div class="metrics">
        <div class="metric latency very-slow">
          <span class="k">outcome</span>
          <span class="v">refused</span>
        </div>
      </div>
      <div class="error-block">${escapeHtml(msg)}

The query never even runs. Weaviate refuses upfront because the property has neither indexFilterable nor indexRangeFilters enabled, so there is no inverted bucket to consult. Flip the relevant flag from the Weaviate console, wait for runtime reindex to finish, and re-run.</div>
    `;
    return;
  }
  const body = err.body ? JSON.stringify(err.body, null, 2) : err.stack || String(err);
  container.innerHTML = `<div class="error-block">${escapeHtml(msg)}\n\n${escapeHtml(body)}</div>`;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[c]));
}

function renderResults(container, opts) {
  const { latencyMs, totalCount, totalCountLabel, rows, renderRow, emptyNote } = opts;
  const lc = latencyClass(latencyMs);
  const metrics = [];
  metrics.push(`
    <div class="metric latency ${lc}">
      <span class="k">latency (wall-clock)</span>
      <span class="v">${fmtMs(latencyMs)}</span>
    </div>`);
  if (totalCount != null) {
    metrics.push(`
      <div class="metric">
        <span class="k">${escapeHtml(totalCountLabel || "matching objects")}</span>
        <span class="v">${totalCount.toLocaleString()}</span>
      </div>`);
  }
  metrics.push(`
    <div class="metric">
      <span class="k">returned (top)</span>
      <span class="v">${rows.length}</span>
    </div>`);

  const list = rows.length
    ? `<ul class="result-list">${rows.map(renderRow).join("")}</ul>`
    : `<p class="placeholder">${escapeHtml(emptyNote || "No results.")}</p>`;

  container.innerHTML = `<div class="metrics">${metrics.join("")}</div>${list}`;
}

function rowGeneric(obj, keyField) {
  const keyVal = obj[keyField] ?? "(missing)";
  const meta = [];
  if (obj.name) meta.push(`name: ${escapeHtml(obj.name)}`);
  if (obj.brand) meta.push(`brand: ${escapeHtml(obj.brand)}`);
  if (obj.category) meta.push(`category: ${escapeHtml(obj.category)}`);
  if (obj.price_cents != null) meta.push(`price: ${fmtPrice(obj.price_cents)}`);
  if (obj.sku && keyField !== "sku") meta.push(`sku: ${escapeHtml(obj.sku)}`);
  return `<li>
    <span class="row-key">${escapeHtml(String(keyVal))}</span>
    <span class="row-meta">${meta.join("  -  ")}</span>
  </li>`;
}

// -- Card wiring -------------------------------------------------------------

async function runWithButton(button, container, fn) {
  button.disabled = true;
  const previousLabel = button.textContent;
  button.textContent = "Running...";
  container.innerHTML = `<p class="placeholder">Running query...</p>`;
  try {
    await fn();
  } catch (err) {
    renderError(container, err);
  } finally {
    button.textContent = previousLabel;
    button.disabled = false;
  }
}

document.querySelectorAll(".run-btn").forEach(btn => {
  btn.addEventListener("click", () => onRun(btn));
});

async function onRun(btn) {
  const action = btn.dataset.action;
  const card = btn.closest(".card");
  const cardName = card.dataset.card;
  const container = card.querySelector(".result");

  switch (action) {
    case "run-price-range":
      return runWithButton(btn, container, async () => {
        // Issue both in parallel - aggregate hits the same brute-force path
        // as Get, so the slow one wins. We report the slower wall-clock so
        // the user sees the worst-case cost of the misconfiguration.
        const [get, count] = await Promise.all([
          timedGql(qPriceRange()),
          timedGql(qPriceRangeCount()),
        ]);
        const rows = get.body.data.Get[COLLECTION] || [];
        const total = count.body.data.Aggregate[COLLECTION][0]?.meta?.count ?? null;
        renderResults(container, {
          latencyMs: Math.max(get.ms, count.ms),
          totalCount: total,
          totalCountLabel: "objects with price_cents in 30000..40000",
          rows,
          renderRow: r => rowGeneric(r, "price_cents") || "",
          emptyNote: "Range query returned no rows."
        });
      });

    case "run-path-bm25":
      return runWithButton(btn, container, async () => {
        const { body, ms } = await timedGql(qPathBm25());
        const rows = body.data.Get[COLLECTION] || [];
        renderResults(container, {
          latencyMs: ms,
          rows,
          renderRow: r => rowGeneric(r, "spec_sheet_path"),
          emptyNote: "BM25 returned no rows."
        });
      });

    case "run-sku-bm25":
      return runWithButton(btn, container, async () => {
        const { body, ms } = await timedGql(qSkuBm25());
        const rows = body.data.Get[COLLECTION] || [];
        renderResults(container, {
          latencyMs: ms,
          rows,
          renderRow: r => rowGeneric(r, "sku"),
          emptyNote: "BM25 returned no rows."
        });
      });

    case "run-category-equal":
      return runWithButton(btn, container, async () => {
        const [get, count] = await Promise.all([
          timedGql(qCategoryEqual()),
          timedGql(qCategoryEqualCount()),
        ]);
        const rows = get.body.data.Get[COLLECTION] || [];
        const total = count.body.data.Aggregate[COLLECTION][0]?.meta?.count ?? null;
        renderResults(container, {
          latencyMs: Math.max(get.ms, count.ms),
          totalCount: total,
          totalCountLabel: 'objects with category == "Cameras > Action"',
          rows,
          renderRow: r => rowGeneric(r, "category"),
          emptyNote: "No matching rows."
        });
      });

    case "run-email-bm25":
      return runWithButton(btn, container, async () => {
        const { body, ms } = await timedGql(qEmailBm25());
        const rows = body.data.Get[COLLECTION] || [];
        renderResults(container, {
          latencyMs: ms,
          rows,
          renderRow: r => rowGeneric(r, "support_email"),
          emptyNote: "BM25 returned no rows."
        });
      });
  }
}
