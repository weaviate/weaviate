# Drop Vector Index — Operations

Operator-facing knobs and behaviors for the drop-vector-index feature
(`DELETE /v1/schema/{className}/vectors/{vectorIndexName}/index`). Design
details live in the internal RFC; this page covers what an operator can tune
and observe.

## `DROP_VECTOR_INDEX_RECONCILE_INTERVAL_SECONDS`

Interval of the periodic drop-vector reconciliation loop (leader-only).
Reconciliation is the pickup path for drop markers whose cleanup could not
finish in one round: tenants that were inactive (COLD/FROZEN) when the
cleanup ran, tenants created mid-drop, failed rounds, and finalizes that were
deferred. Most rounds are also triggered immediately by task completions
(a nudge), so the interval is the *fallback* cadence, not the typical
latency.

- **Default:** 900 (15 minutes).
- **Bounds:** 1 … 604800 (7 days). Out-of-range or unparsable values fail
  startup.
- **When to lower it:** after restores or bulk tenant activations, when you
  want markers on previously-inactive tenants to converge in seconds rather
  than minutes. Each round costs one leader-consistent task-list read plus
  schema reads per pending marker; on clusters with no pending drop markers a
  round is effectively free.

## Related behavior worth knowing

- A drop on a collection with inactive tenants completes for the active ones
  and leaves the schema marker until every tenant has been cleaned; the
  marker is picked up automatically when tenants are activated (see the
  reconcile interval above).
- Completed cleanup records are retained past
  `DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS` while their marker is still
  pending — they carry the coverage bookkeeping that prevents re-cleaning
  already-cleaned tenants.
- Dropping a collection's **last** named vector converts the collection to
  the vector-less shape (`vectorizer: none`, no named vectors) — it never
  inherits `DEFAULT_VECTORIZER_MODULE`.
