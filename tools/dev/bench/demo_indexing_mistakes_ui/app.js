// Demo UI for the "Common Indexing Mistakes" build.
//
// The cards display Python client snippets so viewers see what they should
// actually use in their own code; the transport here is GraphQL only because
// the browser can hit /v1/graphql directly without a Python runtime. No
// client-side caching - every press of "Run query" hits the server fresh.
//
// Every request is sent to /api/* on the same origin. start.py proxies
// /api/* to the dev cluster, injecting the Bearer token from the
// WCD_API_KEY env var server-side. The API key never reaches the
// browser, and the cluster's CORS policy is irrelevant.

const API_PREFIX = "/api";
const CLASS = "IndexingMistakesDemo";

const connLight = document.getElementById("conn-light");
const connStatusText = document.getElementById("conn-status-text");
const resetBtn = document.getElementById("reset-btn");
const resetStatus = document.getElementById("reset-status");

// -- Connection helpers ------------------------------------------------------

function apiUrl(path) {
  return API_PREFIX + path;
}
function authHeaders() {
  return { "content-type": "application/json" };
}

async function pingServer() {
  try {
    const r = await fetch(apiUrl("/v1/meta"), { headers: authHeaders(), cache: "no-store" });
    if (!r.ok) throw new Error(`status ${r.status}`);
    const meta = await r.json();
    connLight.className = "conn-light ok";
    connStatusText.textContent = `connected (v${meta.version || "?"})`;
    return true;
  } catch (e) {
    connLight.className = "conn-light bad";
    connStatusText.textContent = `unreachable: ${e.message}`;
    return false;
  }
}
pingServer();

// -- GraphQL transport -------------------------------------------------------

async function gqlRaw(query) {
  const r = await fetch(apiUrl("/v1/graphql"), {
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

// -- Reset (re-break the schema) --------------------------------------------
//
// After the viewer fixes each mistake from the Weaviate console, this button
// reverts each property to its broken state so the demo can be re-run.
// Operations on distinct properties fire in parallel (the per-property submit
// lock added in a7bb362854 lets them through); the two price_cents deletions
// run sequentially against that same lock.
//
//   1. price_cents:     DELETE the rangefilters index, then the columnar one
//   2. category:        DELETE the filterable index
//   3. spec_sheet_path: change tokenization back to "word"

async function deleteIndex(prop, indexType) {
  const url = apiUrl(`/v1/schema/${CLASS}/properties/${prop}/index/${indexType}`);
  const r = await fetch(url, { method: "DELETE", headers: authHeaders(), cache: "no-store" });
  if (r.status === 200 || r.status === 204) return { ok: true, status: r.status, note: "deleted" };
  if (r.status === 404 || r.status === 400) {
    // Already in the broken state (no such index); idempotent no-op.
    return { ok: true, status: r.status, note: "already broken" };
  }
  const body = await r.text();
  return { ok: false, status: r.status, note: body.slice(0, 200) };
}

async function changeTokenization(prop, tokenization) {
  const url = apiUrl(`/v1/schema/${CLASS}/indexes/${prop}`);
  const body = JSON.stringify({ searchable: { tokenization } });
  const r = await fetch(url, { method: "PUT", headers: authHeaders(), body, cache: "no-store" });
  const text = await r.text();
  if (r.status === 400 && /already uses tokenization/i.test(text)) {
    return { ok: true, status: 400, note: "already broken" };
  }
  if (r.status !== 202) {
    return { ok: false, status: r.status, note: text.slice(0, 200) };
  }
  const parsed = JSON.parse(text);
  const taskId = parsed.taskId;
  if (!taskId) return { ok: false, status: r.status, note: "no taskId in 202 body" };
  // Poll until terminal.
  const t0 = Date.now();
  while (Date.now() - t0 < 120_000) {
    await new Promise(res => setTimeout(res, 1000));
    const statusURL = apiUrl(`/v1/schema/${CLASS}/indexes/${prop}`);
    const sr = await fetch(statusURL, { headers: authHeaders(), cache: "no-store" });
    if (!sr.ok) continue;
    const sjson = await sr.json();
    // GET /indexes returns a property entry with `indexes:[{type,status,...}]`.
    const flagOff = (sjson.indexes || []).every(i =>
      i.type !== "searchable" || i.status === "ready"
    );
    if (flagOff) return { ok: true, status: 202, note: `task ${taskId} finished` };
  }
  return { ok: false, status: 202, note: `task ${taskId} did not finish within 120s` };
}

async function doReset() {
  resetBtn.disabled = true;
  const previousLabel = resetBtn.textContent;
  resetBtn.textContent = "Resetting...";
  resetStatus.className = "reset-status visible";
  resetStatus.textContent = "firing 4 operations...";

  const t0 = performance.now();
  const priceCentsChain = (async () => {
    const range = await deleteIndex("price_cents", "rangeFilters");
    const columnar = await deleteIndex("price_cents", "columnar");
    return [range, columnar];
  })();
  const ops = [
    ["price_cents (rangeFilters)",     priceCentsChain.then(r => r[0])],
    ["price_cents (columnar)",         priceCentsChain.then(r => r[1])],
    ["category (filterable)",          deleteIndex("category", "filterable")],
    ["spec_sheet_path (tokenization)", changeTokenization("spec_sheet_path", "word")],
  ];
  const results = await Promise.all(ops.map(([_, p]) => p));
  const elapsed = Math.round(performance.now() - t0);

  const lines = ops.map(([label, _], i) => {
    const r = results[i];
    const mark = r.ok ? "ok" : "FAIL";
    return `[${mark}] ${label} - HTTP ${r.status} - ${r.note}`;
  });
  const allOk = results.every(r => r.ok);
  resetStatus.className = `reset-status visible ${allOk ? "ok" : "bad"}`;
  resetStatus.textContent = `${allOk ? "Reset complete" : "Reset finished with errors"} in ${elapsed} ms\n` + lines.join("\n");

  resetBtn.textContent = previousLabel;
  resetBtn.disabled = false;
}
resetBtn.addEventListener("click", doReset);

// -- Query templates ---------------------------------------------------------

function qPriceRange() {
  return `{
    Get {
      ${CLASS}(
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
      ${CLASS}(
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

// Keep this list in sync with CANONICAL_GOPRO_OBJECTS in
// demo_indexing_mistakes_import.py — the ingest script injects exactly these
// five spec_sheet_path values as canonical records, so the post-migration
// (field-tokenization) state of mistake 2 always returns 5 rows.
const CANONICAL_GOPRO_PATHS = [
  "/products/cameras/gopro/hero-12-black/spec.pdf",
  "/products/cameras/gopro/hero-11-silver/spec.pdf",
  "/products/cameras/gopro/max-360/spec.pdf",
  "/products/cameras/gopro/hero-mini/spec.pdf",
  "/products/cameras/gopro/fusion-4k/spec.pdf",
];

// GraphQL where-filters take scalar values only, so we express "any of these
// 5 paths" as Or(Equal × 5). The Python snippet shown in the UI uses the
// v4 client's `.contains_any([...])`, which the client transport translates
// to the same semantics.
function qPathContainsAny() {
  const operands = CANONICAL_GOPRO_PATHS.map(p =>
    `{ path: ["spec_sheet_path"], operator: Equal, valueText: ${JSON.stringify(p)} }`
  ).join(", ");
  return `{
    Get {
      ${CLASS}(
        where: { operator: Or, operands: [${operands}] },
        limit: 10
      ) {
        spec_sheet_path category brand name
      }
    }
  }`;
}

// Filtered aggregations for cards 4 and 5. The filter property is well
// indexed in both cases - the cost being demonstrated is the aggregation
// itself, which without a columnar index on price_cents fetches and
// unmarshals every matching object just to extract one integer.

function qAggSelective() {
  return `{
    Aggregate {
      ${CLASS}(
        where: { path: ["brand"], operator: Equal, valueText: "GoPro" }
      ) {
        meta { count }
        price_cents { mean minimum maximum sum }
      }
    }
  }`;
}

function qAggBroad() {
  return `{
    Aggregate {
      ${CLASS}(
        where: { path: ["in_stock"], operator: Equal, valueBoolean: true }
      ) {
        meta { count }
        price_cents { mean minimum maximum sum }
      }
    }
  }`;
}

function qCategoryEqual() {
  return `{
    Get {
      ${CLASS}(
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
      ${CLASS}(
        where: { path: ["category"], operator: Equal, valueText: "Cameras > Action" }
      ) { meta { count } }
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
  // Gateway termination: on Weaviate Cloud the object-scan aggregation of
  // card 5 runs longer than the load balancer allows (~60s), so the
  // connection is reset with a 502/503 before the query finishes. That IS
  // the failure mode the card demonstrates - the analytics query is
  // effectively unusable - so frame it as a demo outcome.
  const rawBody = typeof err.body?.raw === "string" ? err.body.raw : "";
  const gatewayKilled =
    /HTTP (502|503|504)/.test(msg) ||
    /upstream connect error|connection termination|gateway/i.test(rawBody);
  if (gatewayKilled) {
    container.innerHTML = `
      <div class="metrics">
        <div class="metric latency very-slow">
          <span class="k">outcome</span>
          <span class="v">connection killed</span>
        </div>
      </div>
      <div class="error-block">${escapeHtml(msg)} - ${escapeHtml(rawBody || "gateway terminated the connection")}

The aggregation ran longer than the cloud gateway allows, so the connection was reset before a result could be returned. The query is effectively unusable at this catalog size. Enable indexColumnar on price_cents from the Weaviate console, wait for the backfill, and re-run - the same aggregation returns in well under a second.</div>
    `;
    return;
  }
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

// Renders the result of a filtered aggregation card: latency, match count,
// and one row per requested metric on price_cents.
function renderAggResults(container, { latencyMs, matched, metrics }) {
  const lc = latencyClass(latencyMs);
  const metricBoxes = [`
    <div class="metric latency ${lc}">
      <span class="k">latency (wall-clock)</span>
      <span class="v">${fmtMs(latencyMs)}</span>
    </div>`];
  if (matched != null) {
    metricBoxes.push(`
      <div class="metric">
        <span class="k">objects matched by filter</span>
        <span class="v">${matched.toLocaleString()}</span>
      </div>`);
  }
  const rows = metrics.map(([label, value]) => `<li>
    <span class="row-key">${escapeHtml(label)}</span>
    <span class="row-meta">${escapeHtml(value)}</span>
  </li>`);
  container.innerHTML =
    `<div class="metrics">${metricBoxes.join("")}</div>` +
    `<ul class="result-list">${rows.join("")}</ul>`;
}

function fmtPriceMetric(cents) {
  if (cents == null) return "(null)";
  return `$${(cents / 100).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

async function runAggCard(container, query) {
  const { body, ms } = await timedGql(query);
  const agg = body.data.Aggregate[CLASS][0] || {};
  const pc = agg.price_cents || {};
  renderAggResults(container, {
    latencyMs: ms,
    matched: agg.meta?.count ?? null,
    metrics: [
      ["mean price", fmtPriceMetric(pc.mean)],
      ["min price", fmtPriceMetric(pc.minimum)],
      ["max price", fmtPriceMetric(pc.maximum)],
      ["sum", fmtPriceMetric(pc.sum)],
    ],
  });
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
  const container = card.querySelector(".result");

  switch (action) {
    case "run-price-range":
      return runWithButton(btn, container, async () => {
        const [get, count] = await Promise.all([
          timedGql(qPriceRange()),
          timedGql(qPriceRangeCount()),
        ]);
        const rows = get.body.data.Get[CLASS] || [];
        const total = count.body.data.Aggregate[CLASS][0]?.meta?.count ?? null;
        renderResults(container, {
          latencyMs: Math.max(get.ms, count.ms),
          totalCount: total,
          totalCountLabel: "objects with price_cents in 30000..40000",
          rows,
          renderRow: r => rowGeneric(r, "price_cents") || "",
          emptyNote: "Range query returned no rows."
        });
      });

    case "run-path-any-of":
      return runWithButton(btn, container, async () => {
        const { body, ms } = await timedGql(qPathContainsAny());
        const rows = body.data.Get[CLASS] || [];
        renderResults(container, {
          latencyMs: ms,
          rows,
          renderRow: r => rowGeneric(r, "spec_sheet_path"),
          emptyNote: "Any-of filter returned no rows."
        });
      });

    case "run-category-equal":
      return runWithButton(btn, container, async () => {
        const [get, count] = await Promise.all([
          timedGql(qCategoryEqual()),
          timedGql(qCategoryEqualCount()),
        ]);
        const rows = get.body.data.Get[CLASS] || [];
        const total = count.body.data.Aggregate[CLASS][0]?.meta?.count ?? null;
        renderResults(container, {
          latencyMs: Math.max(get.ms, count.ms),
          totalCount: total,
          totalCountLabel: 'objects with category == "Cameras > Action"',
          rows,
          renderRow: r => rowGeneric(r, "category"),
          emptyNote: "No matching rows."
        });
      });

    case "run-agg-selective":
      return runWithButton(btn, container, () => runAggCard(container, qAggSelective()));

    case "run-agg-broad":
      return runWithButton(btn, container, () => runAggCard(container, qAggBroad()));
  }
}
