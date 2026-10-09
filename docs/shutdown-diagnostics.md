# Diagnosing a slow node shutdown

A node logs what its shutdown did and, while a shutdown takes long, what it is still waiting on. This
page explains those lines and how to get from "the shutdown was slow" to a cause and an action.

Every line described here carries `"action":"shutdown_report"` and `node` (the node name), so one
filter collects them from all nodes:

```
jq -cR 'fromjson? | select(.action?=="shutdown_report")'
```

With `LOG_FORMAT=text`, filter with `grep 'action=shutdown_report'` instead.

## What gets logged

| Message | Level | When |
|---|---|---|
| `shutdown finished` | Info | Once per shutdown that completes. Always logged, also for fast shutdowns. |
| `shutdown still running` | Info, then Warn | From `SHUTDOWN_HEARTBEAT_AFTER` (15s) after SIGTERM, then every `SHUTDOWN_HEARTBEAT_INTERVAL` (10s), until the shutdown ends. Warn once the shutdown has run longer than `SHUTDOWN_SLOW_LOG_TOTAL_THRESHOLD` (30s). The only `shutdown_report` line written *during* the shutdown, so it survives the process being killed, together with the live lines listed at the end of this page. |
| `shutdown phases took longer than their threshold` | Warn | A phase exceeded its threshold. `phases_over` names them; each has `<phase>_ms` and `<phase>_threshold_ms`. When the total is over, `slowest_step` and `slowest_step_ms` name the longest single step, which can be `client_drain`. |
| `gRPC server stop was forced` | Warn | gRPC clients did not finish within 20s and the server was stopped hard. |
| `REST server close was forced` | Warn | An HTTP request was still running at the graceful timeout (15s, `--graceful-timeout`) and its server was closed. |
| `shutdown skipped flushes or snapshots` | Warn | A bucket did not flush its memtable (shutdown budget expired, or the flush failed), or an async-replication hash tree was not saved. |
| `shutdown skipped shards still in use` | Warn | A shard was still held by a request when the database closed, so its teardown was deferred until that request releases it. |
| `shutdown did not close the database` | Warn | The cluster close ran, but the database close was never reached. |

Apart from the heartbeat, each Warn kind appears at most once per shutdown, written right after the
summary; several kinds can appear in the same shutdown. Counts are fields, never one line per item.

A shutdown with nothing unusual that ends within `SHUTDOWN_HEARTBEAT_AFTER` logs only the `shutdown
finished` line. A longer shutdown also logs heartbeats, at Info while it stays under the total threshold.

### Settings

| Env var | Default | Meaning |
|---|---|---|
| `SHUTDOWN_HEARTBEAT_AFTER` | `15s` | First heartbeat after SIGTERM. `0` disables the heartbeat. |
| `SHUTDOWN_HEARTBEAT_INTERVAL` | `10s` | Time between heartbeats. At least `1s`. |
| `SHUTDOWN_SLOW_LOG_CLIENT_DRAIN_THRESHOLD` | `15s` | Draining client requests (REST and gRPC). |
| `SHUTDOWN_SLOW_LOG_INTERNAL_CLOSE_THRESHOLD` | `10s` | Closing the internal (node-to-node) server. |
| `SHUTDOWN_SLOW_LOG_DB_THRESHOLD` | `30s` | Closing the database (all collections, shards and buckets). |
| `SHUTDOWN_SLOW_LOG_TOTAL_THRESHOLD` | `30s` | The whole shutdown. |

Values are Go durations (`500ms`, `45s`, `2m`). Invalid values stop the node from starting. The
defaults are starting points; adjust them to your deployment.

## Step 1 — Did the shutdown finish?

| You see | Meaning | Next |
|---|---|---|
| `shutdown finished` | It completed. The summary has the full picture. | Step 2. |
| `shutdown still running` lines but no summary | The process was **killed** before finishing — on Kubernetes, SIGKILL after `terminationGracePeriodSeconds` (container exit code 137). | The **last heartbeat** says where it was stuck: `running`, the request counts, `current_index`. Check the pod's grace period: the Kubernetes default is 30s; the Weaviate Helm chart sets 600s. |
| no `shutdown_report` line at all | The process died before `SHUTDOWN_HEARTBEAT_AFTER` (15s), the heartbeat is disabled (`SHUTDOWN_HEARTBEAT_AFTER=0`) and the process died before finishing, or the logs were cut. | Check the pod events, the grace period and the heartbeat setting. |

## Step 2 — Which phase was slow, and what to do

Read `phases_over` (or the largest `<step>_ms` in the summary), then find the row below.

| Signal | Likely cause | Look at | What can be done |
|---|---|---|---|
| `client_drain` over; heartbeats show `rest_requests_running>0`; `rest_forced_close=true` | A REST request kept running after it was cancelled. Requests still running 5s into the shutdown are cancelled and answered 503 (counted in `rest_unavailable`); a forced close at the 15s graceful timeout means a handler did not stop when cancelled — typically a large batch whose objects are already stored, which finishes writing their vectors so no object is left without one. With `rest_requests_running=0` and `grpc_calls_running>0`, the request holding the REST server is a grpc-web call: it is served on the REST port but counted as a gRPC call. | Client batch sizes; vectorizer latency at that time. | Smaller client batches. Bounded at twice the graceful timeout (30s by default): 15s until the servers are closed, then up to 15s for their handlers to return. |
| `client_drain` over; `wait_grpc_stop_ms` high; `grpc_calls_running>0`; `grpc_forced_stop=true` | gRPC batches that were already writing to replicas, waiting for **other nodes** to answer. | **The other nodes' logs at the same time** — were they restarting or busy (see `internal_close` below)? | Usually a symptom of the peers. Bounded at 20s. |
| `client_drain` high but no heartbeat shows requests running | First check `batch_drain_ms`: the batch-stream drain runs before the REST drain and counts towards `client_drain_ms`. If instead `wait_grpc_stop_ms` is high with `grpc_calls_running=0`, gRPC's graceful stop is closing idle client connections, which takes at most about 5s each, in parallel. Longer than that with `grpc_calls_running=0` points to calls that fell between heartbeat samples — the request counts are sampled once per heartbeat. | Batch-stream clients; gRPC client libraries and connection settings. | Both are bounded (10s and 20s); report idle-connection waits if frequent. |
| `batch_drain_ms` high | Batch-stream (`batch.stream()`) clients not finishing. | Stream client versions. | Bounded at 10s. |
| `stop_telemetry_ms` high | The telemetry endpoint is unreachable (outbound traffic blocked). | Egress from the pod. | `DISABLE_TELEMETRY=true`, or allow egress. Bounded at 10s. |
| `internal_close` over; heartbeats show `internal_requests_running>0` | **Replicated writes from other nodes** queued on this node just before shutdown. Each write inserts vectors before it completes, and the close waits for them up to its 60s budget. Common in a rolling restart under write load: the node restarted before pushes its traffic onto the others. | Write load during the rollout; the previous node's `client_drain`; CPU usage on this node. | Roll at a lower write rate; give nodes more CPU. |
| `cluster_close_ms` much larger than `db_ms` | Raft: leadership transfer, raft or membership shutdown. | Raft log lines just before the summary. | Rare. Collect data (step 4). |
| `db` over; `slowest_indexes`, `slowest_shards` | Many shards loaded this lifetime (the teardown writes and syncs every bucket of every loaded shard), or one large shard. | `shards_torn_down`; the slowest shards; the disk type. | Faster disks; fewer active tenants (deactivate or offload idle ones). |
| `buckets_ctx_expired>0` (skipped flushes) | The database ran out of its shutdown budget. That budget (60s) also covers `internal_close` and `cluster_close`, because the database closes inside the cluster close, after raft and membership shut down. A long internal or raft close leaves less for the database. | `internal_close_ms`, and `cluster_close_ms` minus `db_ms`, in the same summary. That difference slightly overstates what came out of the database's budget, because it also includes closing the raft RPC client and server after the database. | Nothing is lost — the data is in the write-ahead log — but the **next startup** replays it and takes longer (step 3). |
| `shards_still_in_use>0` | A request held a shard when the database closed: a long query, a batch, a backup, async replication. The shard is torn down when that request releases it. That teardown is counted in `shards_torn_down` only if it ends before the summary is logged. | What was running at SIGTERM (backups, long queries). | Nothing, if the request finishes before the process exits. If the process exits first, the shard is recovered from its write-ahead log at the next startup. |
| `hashtree_dumps_skipped>0` | A shard's store did not close cleanly, or the async-replication hash tree could not be saved in time, or saving it failed. | The skipped-flush counts on the same Warn; `store hashtree failed` errors or `async replication worker did not stop within deadline` warnings before the summary. | The hash tree is rebuilt at startup, which takes longer. |
| `db_reached=false` (`did not close the database`) | The database close never ran, for example a SIGTERM while the node was still starting. | Errors just before the summary (`close log store`, raft). | The next startup replays all write-ahead logs. Collect data (step 4). |

## Step 3 — Look beyond the one node

- **A slow node is often slowed by its peers.** In a rolling restart, the node that drains clients
  slowly pushes load onto the others, and the next node to stop then shows a long `internal_close`.
  Collect the `shutdown_report` lines from **all nodes in the rollout** and put them on one timeline
  by `time` and `node`.
- **A fast shutdown can make the next startup slow.** Skipped flushes, shards still in use and an
  unclosed database all move work to the next start. Check that node's next time to ready and the
  `startup_durations_ms` metric (`docs/metrics.md`).

## Step 4 — What to collect when this page does not explain it

- All `shutdown_report` lines from every node involved, and the pod events (exit codes, grace period).
- Weaviate version, the `SHUTDOWN_*` settings, and the cluster shape: node count, replication factor,
  collections and tenants, whether async replication or async indexing is enabled.
- The load during the shutdown: write and query rates, batch sizes, client types.
- If it reproduces: set `DEBUG_ENDPOINTS_ENABLED=true` and take goroutine dumps
  (`/debug/pprof/goroutine?debug=2`) every few seconds during the shutdown. They are served on the
  separate debug listener, port 6060 by default (`GO_PROFILING_PORT`); `GO_PROFILING_DISABLE` turns
  that listener off. On Kubernetes use `kubectl port-forward pod/<pod> 6060:6060` — a terminating pod
  is removed from its Service, so a Service port-forward stops reaching it.

## Reference

### The heartbeat's `running` field

`running` lists every step in progress, sorted and comma-joined, so it often holds several names.
Groups appear together with their members (`pre_db_steps` with the phase-one steps, `cluster_close`
with `db`); the innermost name is the useful one.

| Order | Steps |
|---|---|
| Client drain | `client_drain` (until REST drained), `batch_drain` |
| Phase one, concurrent, inside `pre_db_steps` | `leave_cluster`, `wait_grpc_stop`, `drain_exports`, `stop_telemetry`, `close_task_scheduler` |
| Phase two, one after another | `internal_close`, `cluster_close` (contains `db`), `close_db_users`, `close_modules`, `close_conn_managers` |
| Phase three | `shutdown_otel`, `flush_sentry` |

The heartbeat also carries `elapsed_ms`, `current_index` (the collection being closed),
`indexes_done`, `indexes_total` (once known), `shards_in_teardown`, `shards_torn_down`,
`buckets_done`, `rest_requests_running`, `grpc_calls_running` and `internal_requests_running`.

### Summary keys

Keys are stable: they may be added in later versions, never renamed.

| Key | Meaning |
|---|---|
| `total_ms` | SIGTERM handling start to the summary. |
| `client_drain_ms` | Until both REST requests drained and the gRPC server stopped. |
| `<step>_ms` | One per step listed above (`batch_drain_ms` … `flush_sentry_ms`, and `pre_db_steps_ms`). 0 when the step did not run. `cluster_close_ms` includes `db_ms`. |
| `grpc_stop_ms`, `grpc_forced_stop` | gRPC graceful-stop duration, and whether it was forced. |
| `rest_forced_close`, `rest_handlers_still_running` | Whether HTTP servers were closed at the graceful timeout, and handlers still running a further graceful timeout (15s) after that close. |
| `rest_unavailable`, `grpc_unavailable`, `grpc_web_unavailable` | Calls answered "unavailable" during shutdown (refused, or cut while running). Clients retry these. A grpc-web call cut while running can be counted in both grpc_unavailable and grpc_web_unavailable. |
| `db_ms`, `db_reached` | Database close duration, and whether it was reached. |
| `indexes_done`, `indexes_total` | Collections closed / held. |
| `shards_torn_down`, `shards_in_teardown`, `shards_still_in_use` | Shard teardowns finished, started but unfinished, skipped as still in use. |
| `buckets_flushed`, `buckets_wal_reused`, `buckets_ctx_expired`, `buckets_failed` | Per-bucket outcome: flushed to a segment, write-ahead log kept (small memtable), skipped on the expired budget, failed. |
| `hashtree_dumps_skipped` | Async-replication hash trees not saved. |
| `slowest_indexes`, `slowest_shards` | Up to 3 each, `name=duration`, slowest first. |

Lines logged as things happen, independent of the summary, are also useful on a timeline:
`grpc graceful stop timed out, forcing stop`, `N HTTP handlers still running … after their servers
closed`, `refused N REST requests and N gRPC calls …`, and the phase lines `shutdown steps that use
the database returned after …` / `database teardown steps returned after …`.
