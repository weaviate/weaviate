# Usage Limits / Free-Tier Guardrails

Server-side guardrails that bound the resources a single Weaviate instance can consume. Designed for the upcoming Weaviate Cloud Free Tier; usable in any deployment.

All limits are **opt-in**: env vars unset means no enforcement.

## Environment variables

| Variable | Type | Default | Effect |
|---|---|---|---|
| `USAGE_LIMITS_SCOPE` | string | `node` | Unit of accounting. Only `node` is implemented. `cluster` and `namespace` are reserved values; the server refuses to boot if they are set. |
| `USAGE_LIMITS_ERROR_MESSAGE` | string | `"{limit} count limit of {value} reached for this instance."` | Operator-overridable template for the user-facing error message. Placeholders: `{limit}` (resource type) and `{value}` (configured threshold). |
| `MAXIMUM_ALLOWED_OBJECTS_COUNT` | int | `-1` (unlimited) | Cap on live object count per instance. Checked on every single + batch insert. |
| `MAXIMUM_ALLOWED_COLLECTIONS_COUNT` | int | `-1` (unlimited) | Cap on number of collections. Pre-existing env var; behavior preserved. |
| `MAXIMUM_ALLOWED_TENANTS_PER_COLLECTION` | int | `-1` (unlimited) | Cap on tenants per multi-tenant collection. Checked at tenant-create time only. |
| `MAXIMUM_ALLOWED_SHARDS_PER_COLLECTION` | int | `-1` (unlimited) | Cap on `desiredCount` of a class create request's `shardingConfig`. Config-time check. |

All values are **runtime-overrideable** via the existing runtime overrides YAML file (see `RUNTIME_OVERRIDES_*`). Field names in the YAML are the lowercase-snake-case forms of the env-var names.

## Error response

When any limit is hit, Weaviate returns:

- **HTTP**: `429 Too Many Requests` with body
  ```json
  {
    "errorCode": "USAGE_LIMIT_EXCEEDED",
    "limit": "objects",
    "value": 10000,
    "message": "Object count limit of 10000 reached for this instance."
  }
  ```
- **gRPC**: `codes.ResourceExhausted` with `errdetails.ErrorInfo` carrying the same `limit`/`value`/`message` fields under `Reason="USAGE_LIMIT_EXCEEDED"`, `Domain="weaviate.usagelimits"`.

The structured fields (`errorCode`, `limit`, `value`) are stable contract regardless of the `USAGE_LIMITS_ERROR_MESSAGE` template — only the human-facing `message` text changes.

### Batch behavior

When a batch insert would exceed `MAXIMUM_ALLOWED_OBJECTS_COUNT`, the **whole batch is rejected**. No partial fill. The client decides what to retry. This applies to both REST and gRPC batch paths.

## Backward-compat note: collections-limit status code

The pre-existing `MAXIMUM_ALLOWED_COLLECTIONS_COUNT` enforcement previously returned **HTTP 422 Unprocessable Entity** with a free-text "maximum number of collections" message. As of this release it returns **HTTP 429** with the structured `USAGE_LIMIT_EXCEEDED` body described above. Clients matching on the prior 422 status or message text must adapt.

## Accepted imperfections

- **Object count via async path.** Counts come from `CountAsync` and exclude the in-memory memtable, so during fast bulk imports the count lags slightly behind on-disk state. Bounded by in-flight write volume between count refreshes; self-corrects on the next flush.
- **`scope=node` only.** The `cluster` and `namespace` scopes are reserved API surface for future cluster-wide and per-namespace enforcement. Today they fail at startup with `"USAGE_LIMITS_SCOPE=<value> is not yet supported"`.
- **Tenants checked at create time only**, not on subsequent multi-tenancy config changes.
