#!/usr/bin/env bash
# async_checkpoint.sh - manage async replication checkpoints across a Weaviate cluster.
#
# Each Weaviate node exposes a cluster-internal HTTP API (CLUSTER_DATA_BIND_PORT).
# This script fans out checkpoint operations to all nodes and aggregates results.
#
# USAGE
#   async_checkpoint.sh <command> [options]
#
# COMMANDS
#   create   --nodes <h:p,...> --rest <h:p> --class <C> --in <duration> [--shards s1,s2]
#            Create (or overwrite) a checkpoint with cutoff = now + <duration>.
#            <duration> examples: 1h, 30m, 2h30m, 90s
#
#   status   --nodes <h:p,...> --rest <h:p> --class <C> [--shards s1,s2]
#            Show per-shard checkpoint state from every node.
#
#   delete   --nodes <h:p,...> --rest <h:p> --class <C> [--shards s1,s2]
#            Remove the active checkpoint from all nodes (idempotent).
#
#   wait     --nodes <h:p,...> --rest <h:p> --class <C> [--shards s1,s2] [--timeout <sec>]
#            Poll status until every shard has the same root on all nodes.
#            Prints progress on each poll. Exits 0 on convergence, 1 on timeout.
#
# OPTIONS
#   --nodes   Comma-separated list of host:port pairs (cluster API / CLUSTER_DATA_BIND_PORT).
#             Example: localhost:7101,localhost:7103,localhost:7105
#   --rest    One REST API host:port used to discover shard names when --shards is omitted.
#             Example: localhost:8080
#   --class   Collection (class) name (required for all commands).
#   --in      Duration until cutoff for 'create'. Accepts Nh, Nm, Ns, NhMm,
#             etc.; a bare integer with no unit is treated as seconds.
#   --shards  Optional comma-separated shard names. Omit to auto-discover via --rest.
#   --timeout Seconds to wait before giving up (default: 300). Used by 'wait'.
#   --auth    BasicAuth credentials (user:pass) for the cluster-internal API
#             when cluster.auth.basic is enabled on the server. May also be
#             supplied via the WEAVIATE_CLUSTER_AUTH env var. Not applied to
#             the --rest shard-discovery call (a normal REST endpoint).
#
# ROLLING UPGRADES
#   The /replicas/indices/<class>/async-checkpoint endpoint is only present on
#   Weaviate versions that ship this feature. During a rolling upgrade some
#   nodes may still be on an older build and will return HTTP 404 for the
#   create/status/delete calls. That is reported as "[warn] node (HTTP 404)";
#   it's not a correctness issue — older replicas just don't hold a
#   checkpoint and continue using the unbounded hashtree for convergence.
#   Wait until the rollout is complete before relying on cluster-wide
#   convergence in the status output. Convergence between already-upgraded
#   nodes is unaffected.
#
# PRE-REQS ON THE TARGET COLLECTION
#   - replicationFactor > 1
#   - asyncEnabled: true on the collection's replicationConfig
#   Without both, the shard's hashtree is never initialised and create
#   returns HTTP 412 ("async replication is not active on this shard").
#
# DURABILITY
#   Checkpoints are in-memory only. They are NOT persisted to disk and are
#   NOT replicated through RAFT, so a node restart (or process kill) drops
#   the active checkpoint on that node. A graceful 'stop' (DisableAsync
#   Replication, shard shutdown) clears it too. If a checkpoint must
#   survive a node restart, re-create it after the node rejoins the
#   cluster; use the 'wait' command to confirm convergence before relying
#   on the new root.
#
# DEPENDENCIES: curl, jq, date (GNU coreutils or BSD)

set -euo pipefail

# -- colour helpers -----------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

# Informational helpers all go to stderr so functions that compose output
# via $(...) (e.g. collect_status returning JSON) don't accidentally inherit
# diagnostic lines as data. err() was already on stderr; align the rest.
info()    { echo -e "${CYAN}[info]${RESET} $*" >&2; }
ok()      { echo -e "${GREEN}[ok]${RESET}   $*" >&2; }
warn()    { echo -e "${YELLOW}[warn]${RESET} $*" >&2; }
err()     { echo -e "${RED}[err]${RESET}  $*" >&2; }
die()     { err "$*"; exit 1; }
bold()    { echo -e "${BOLD}$*${RESET}" >&2; }

usage() {
    sed -n '/^# USAGE/,/^# DEPENDENCIES/{ /^#/{ s/^# \{0,1\}//; p }; /^[^#]/q }' "$0"
}

# -- dependency check --------------------------------------------------------
for dep in curl jq; do
    command -v "$dep" &>/dev/null || die "required tool not found: $dep"
done

# -- duration parser ---------------------------------------------------------
# parse_duration <str> - prints seconds
# Accepts: 1h, 30m, 90s, 2h30m, 1h15m30s
parse_duration() {
    local input="$1" total=0 remaining="$1" matched=0
    [[ -z "$input" ]] && die "--in: empty duration"
    if [[ "$remaining" =~ ^([0-9]+)h ]]; then
        total=$(( total + BASH_REMATCH[1] * 3600 )); remaining="${remaining#*h}"; matched=1
    fi
    if [[ "$remaining" =~ ^([0-9]+)m ]]; then
        total=$(( total + BASH_REMATCH[1] * 60 )); remaining="${remaining#*m}"; matched=1
    fi
    if [[ "$remaining" =~ ^([0-9]+)s$ ]]; then
        # explicit seconds component (90s, or the trailing s of 2h30m15s)
        total=$(( total + BASH_REMATCH[1] )); remaining=""; matched=1
    elif [[ "$matched" -eq 0 && "$remaining" =~ ^([0-9]+)$ ]]; then
        # bare integer with no unit = seconds (convenience: --in 300)
        total=$(( total + BASH_REMATCH[1] )); remaining=""; matched=1
    fi
    [[ "$matched" -eq 0 || -n "$remaining" ]] && die "--in: cannot parse '$input' (use e.g. 1h, 30m, 2h30m)"
    [[ "$total" -le 0 ]] && die "--in: duration must be positive"
    echo "$total"
}

# -- argument parsing --------------------------------------------------------
COMMAND="${1:-}"
[[ -z "$COMMAND" ]] && { usage; exit 1; }
shift

NODES="" REST_NODE="" CLASS="" IN_DURATION="" SHARDS_ARG="" TIMEOUT_SEC=300
CLUSTER_AUTH="${WEAVIATE_CLUSTER_AUTH:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes)   NODES="$2";        shift 2 ;;
        --rest)    REST_NODE="$2";    shift 2 ;;
        --class)   CLASS="$2";        shift 2 ;;
        --in)      IN_DURATION="$2";  shift 2 ;;
        --shards)  SHARDS_ARG="$2";   shift 2 ;;
        --timeout) TIMEOUT_SEC="$2";  shift 2 ;;
        --auth)    CLUSTER_AUTH="$2"; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done

[[ -z "$NODES" ]]    && die "--nodes is required"
[[ -z "$CLASS" ]]    && die "--class is required"

IFS=',' read -ra NODE_LIST <<< "$NODES"

# -- shard discovery ---------------------------------------------------------
# resolve_shards - populates SHARD_LIST array and SHARDS_QS / SHARDS_JSON globals
SHARD_LIST=()
SHARDS_QS=""
SHARDS_JSON="[]"

resolve_shards() {
    if [[ -n "$SHARDS_ARG" ]]; then
        IFS=',' read -ra SHARD_LIST <<< "$SHARDS_ARG"
        # Operator scoped the request explicitly — send that exact list.
        SHARDS_JSON=$(printf '%s\n' "${SHARD_LIST[@]}" | jq -R . | jq -s .)
    else
        [[ -z "$REST_NODE" ]] && die "--rest is required when --shards is not specified (needed to auto-discover shards)"
        local resp
        resp=$(curl -sf "http://${REST_NODE}/v1/schema/${CLASS}/shards") \
            || die "failed to discover shards from http://${REST_NODE}/v1/schema/${CLASS}/shards"
        mapfile -t SHARD_LIST < <(echo "$resp" | jq -r '.[].name')
        [[ "${#SHARD_LIST[@]}" -eq 0 ]] && die "no shards found for class '${CLASS}' via ${REST_NODE}"
        info "Auto-discovered ${#SHARD_LIST[@]} shard(s): ${SHARD_LIST[*]}"
        # Send an empty list so create/delete apply to whatever shards each
        # node actually hosts (server expands "empty" to "all local shards").
        SHARDS_JSON="[]"
    fi
    for s in "${SHARD_LIST[@]}"; do
        SHARDS_QS="${SHARDS_QS}&shards=${s}"
    done
}

# -- HTTP helpers ------------------------------------------------------------
base_url() { echo "http://$1/replicas/indices/${CLASS}/async-checkpoint"; }

# CURL_AUTH carries the BasicAuth args for the cluster-internal API; it stays
# empty unless --auth / WEAVIATE_CLUSTER_AUTH is set. Every expansion uses the
# "${CURL_AUTH[@]+...}" guard so an empty array is safe under `set -u`.
CURL_AUTH=()
[[ -n "$CLUSTER_AUTH" ]] && CURL_AUTH=(-u "$CLUSTER_AUTH")

# -- timestamp formatting ----------------------------------------------------
# current_ms - current Unix time in milliseconds.
# GNU date supports %N (nanoseconds); BSD/macOS date does not. macOS users
# who install coreutils get `gdate`. The [[ digits ]] guard rejects BSD
# date's literal "%3N" passthrough and falls through to second resolution.
# Millisecond precision matters because the server tie-breaker rejects a new
# checkpoint whose created_at_ms is not strictly newer (HTTP 409); on a
# second-resolution clock two `create` runs within the same second collide.
current_ms() {
    local ms
    ms=$(gdate +%s%3N 2>/dev/null) && [[ "$ms" =~ ^[0-9]+$ ]] && { echo "$ms"; return; }
    ms=$(date  +%s%3N 2>/dev/null) && [[ "$ms" =~ ^[0-9]+$ ]] && { echo "$ms"; return; }
    echo "$(( $(date +%s) * 1000 ))"   # BSD fallback: second resolution
}

# format_ms <ms> - human-readable UTC timestamp, or "-" if 0
format_ms() {
    local ms="$1"
    [[ "$ms" -le 0 ]] && { echo "-"; return; }
    local sec=$(( ms / 1000 ))
    # Try GNU date (-d), fall back to BSD date (-r)
    date -u -d "@${sec}" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null \
        || date -u -r "${sec}" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null \
        || echo "${ms}ms"
}

# -- status display ----------------------------------------------------------
# print_status_table <aggregated-json>
# aggregated-json: {"shard": {"node:port": {root, cutoff_ms, created_at_ms}, ...}, ...}
print_status_table() {
    local data="$1"
    local shards; shards=$(echo "$data" | jq -r 'keys[]' | sort)

    echo ""
    bold "=== Checkpoint Status ==="
    echo ""

    local all_converged=true

    while IFS= read -r shard; do
        local entries; entries=$(echo "$data" | jq -c ".\"$shard\"")
        local node_count; node_count=$(echo "$entries" | jq 'keys | length')
        local active_count; active_count=$(echo "$entries" | jq '[to_entries[] | select(.value.cutoff_ms > 0)] | length')
        local roots; roots=$(echo "$entries" | jq '[to_entries[] | select(.value.cutoff_ms > 0) | .value.root + ":" + (.value.cutoff_ms|tostring)] | unique | length')

        local shard_status="${GREEN}converged${RESET}"
        if [[ "$active_count" -eq 0 ]]; then
            shard_status="${YELLOW}no checkpoint${RESET}"
            all_converged=false
        elif [[ "$roots" -gt 1 ]]; then
            shard_status="${RED}diverged (${roots} distinct roots)${RESET}"
            all_converged=false
        elif [[ "$active_count" -lt "$node_count" ]]; then
            shard_status="${YELLOW}partial (${active_count}/${node_count} nodes active)${RESET}"
            all_converged=false
        fi

        echo -e "  ${BOLD}Shard:${RESET} $shard   ${BOLD}Status:${RESET} $(echo -e "$shard_status")"
        printf "  %-28s %-22s %-22s %-34s\n" "Node" "Cutoff (UTC)" "Created At (UTC)" "Root (b64)"
        printf "  %-28s %-22s %-22s %-34s\n" "----------------------------" "----------------------" "----------------------" "----------------------------------"

        while IFS= read -r node_entry; do
            local node; node=$(echo "$node_entry" | jq -r '.key')
            local root; root=$(echo "$node_entry" | jq -r '.value.root')
            local cutoff_ms; cutoff_ms=$(echo "$node_entry" | jq -r '.value.cutoff_ms')
            local created_ms; created_ms=$(echo "$node_entry" | jq -r '.value.created_at_ms')

            local cutoff_str; cutoff_str=$(format_ms "$cutoff_ms")
            local created_str; created_str=$(format_ms "$created_ms")
            local root_display="${root:0:32}"
            [[ "$cutoff_ms" -le 0 ]] && root_display="-"

            printf "  %-28s %-22s %-22s %-34s\n" "$node" "$cutoff_str" "$created_str" "$root_display"
        done < <(echo "$entries" | jq -c 'to_entries[]')

        echo ""
    done <<< "$shards"

    $all_converged && return 0 || return 1
}

# -- collect_status - aggregated JSON {"shard":{"node":{...},...},...}
#
# Captures both the response body and HTTP code from each node so the
# well-known status codes used by cmd_create / cmd_delete can be annotated
# the same way here — most importantly 404, which is the rolling-upgrade
# signal documented in the script header. Older builds without this
# endpoint were previously reported as bare "could not reach" lines.
collect_status() {
    local aggregate='{}'
    for node in "${NODE_LIST[@]}"; do
        local out body http_code
        # -w writes "<LF><http_code>" after the body. On a connection
        # failure curl exits non-zero (and writes "000" via -w); the
        # `|| out=$'\n000'` keeps set -e happy.
        out=$(curl -s "${CURL_AUTH[@]+"${CURL_AUTH[@]}"}" -w $'\n%{http_code}' "$(base_url "$node")?${SHARDS_QS#&}") || out=$'\n000'
        http_code="${out##*$'\n'}"
        body="${out%$'\n'*}"

        if [[ "$http_code" != "200" ]]; then
            warn "  $node  (HTTP $http_code)$(annotate_http_code "$http_code") - skipping"
            continue
        fi

        aggregate=$(echo "$aggregate" | jq \
            --argjson resp "$body" \
            --arg node "$node" '
            reduce ($resp | to_entries[]) as $e (
                .;
                .[$e.key][$node] = $e.value
            )
        ')
    done
    echo "$aggregate"
}

# -- commands ----------------------------------------------------------------

# annotate_http_code adds a short hint for well-known status codes so
# operators can distinguish rolling-upgrade noise from real failures
# without having to grep the server logs.
annotate_http_code() {
    case "$1" in
        401) echo " - unauthorized; cluster.auth.basic is enabled - pass --auth user:pass (or set WEAVIATE_CLUSTER_AUTH)" ;;
        404) echo " - endpoint not found; likely an older Weaviate version (rolling upgrade in progress?)" ;;
        409) echo " - stale createdAt; an active checkpoint with a newer createdAt already holds the shard" ;;
        412) echo " - async replication is not active on this shard (factor=1 or asyncEnabled=false?)" ;;
        503) echo " - node not ready or shutting down" ;;
        000) echo " - connection failed (node unreachable)" ;;
        *)   echo "" ;;
    esac
}

cmd_create() {
    [[ -z "$IN_DURATION" ]] && die "create requires --in <duration>"
    resolve_shards
    local dur_sec; dur_sec=$(parse_duration "$IN_DURATION")
    local now_ms; now_ms=$(current_ms)
    local cutoff_ms=$(( now_ms + dur_sec * 1000 ))
    local created_at_ms="$now_ms"

    info "Creating checkpoint for class '${CLASS}'"
    info "  cutoff:     $(format_ms "$cutoff_ms")  (+${IN_DURATION}, ${cutoff_ms} ms)"
    info "  created_at: $(format_ms "$created_at_ms")  (${created_at_ms} ms)"
    info "  shards:     ${SHARD_LIST[*]}"
    echo ""

    local body; body=$(jq -n \
        --argjson shards "$SHARDS_JSON" \
        --argjson cutoff "$cutoff_ms" \
        --argjson created "$created_at_ms" \
        '{shards: $shards, cutoff_ms: $cutoff, created_at_ms: $created}')

    local success=0 failed=0
    for node in "${NODE_LIST[@]}"; do
        local http_code
        http_code=$(curl -s "${CURL_AUTH[@]+"${CURL_AUTH[@]}"}" -o /dev/null -w "%{http_code}" -X POST \
              -H "Content-Type: application/json" -d "$body" "$(base_url "$node")") || true
        if [[ "$http_code" == "200" ]]; then
            ok "  $node  (HTTP $http_code)"
            success=$(( success + 1 ))
        else
            warn "  $node  (HTTP $http_code)$(annotate_http_code "$http_code")"
            failed=$(( failed + 1 ))
        fi
    done

    echo ""
    info "Done: ${success} node(s) accepted, ${failed} failed/unreachable."
}

cmd_delete() {
    resolve_shards
    info "Deleting checkpoint for class '${CLASS}'"
    info "  shards: ${SHARD_LIST[*]}"
    echo ""

    local body; body=$(jq -n --argjson shards "$SHARDS_JSON" '{shards: $shards}')

    local success=0 failed=0
    for node in "${NODE_LIST[@]}"; do
        local http_code
        http_code=$(curl -s "${CURL_AUTH[@]+"${CURL_AUTH[@]}"}" -o /dev/null -w "%{http_code}" -X DELETE \
                    -H "Content-Type: application/json" -d "$body" "$(base_url "$node")") || true
        if [[ "$http_code" == "200" ]]; then
            ok "  $node  (HTTP $http_code)"
            success=$(( success + 1 ))
        else
            warn "  $node  (HTTP $http_code)$(annotate_http_code "$http_code")"
            failed=$(( failed + 1 ))
        fi
    done

    echo ""
    info "Done: ${success} node(s) accepted, ${failed} failed/unreachable."
}

cmd_status() {
    resolve_shards
    info "Fetching checkpoint status for class '${CLASS}' from ${#NODE_LIST[@]} node(s)..."
    local data; data=$(collect_status)
    local shard_count; shard_count=$(echo "$data" | jq 'keys | length')
    [[ "$shard_count" -eq 0 ]] && { warn "No shards returned (no nodes reachable?)."; exit 1; }
    print_status_table "$data" && ok "All shards converged." || true
}

cmd_wait() {
    resolve_shards
    local total_shards="${#SHARD_LIST[@]}"
    info "Waiting for ${total_shards} shard(s) to converge for class '${CLASS}' (timeout: ${TIMEOUT_SEC}s)..."
    echo ""

    local deadline=$(( $(date +%s) + TIMEOUT_SEC ))
    local poll_interval=5
    local attempt=0

    while true; do
        attempt=$(( attempt + 1 ))
        local now; now=$(date +%s)
        local remaining=$(( deadline - now ))
        [[ "$remaining" -le 0 ]] && { echo ""; die "Timed out after ${TIMEOUT_SEC}s - shards did not converge."; }

        local data; data=$(collect_status)
        local seen; seen=$(echo "$data" | jq 'keys | length')

        if [[ "$seen" -eq 0 ]]; then
            warn "[attempt ${attempt}] No shards returned - retrying in ${poll_interval}s  (${remaining}s remaining)"
            sleep "$poll_interval"
            continue
        fi

        # A shard is converged when every queried node has cutoff_ms > 0 and all share the same root
        local converged_shards; converged_shards=$(echo "$data" | jq '
            [to_entries[] |
              select(
                (.value | [to_entries[] | .value.cutoff_ms] | all(. > 0)) and
                (.value | [to_entries[] | .value.root + ":" + (.value.cutoff_ms|tostring)] | unique | length) == 1
              )
            ] | length')

        local pct=0
        [[ "$total_shards" -gt 0 ]] && pct=$(( converged_shards * 100 / total_shards ))

        printf "\r[attempt %d]  %d/%d shards converged (%d%%)  -  %ds remaining   " \
               "$attempt" "$converged_shards" "$total_shards" "$pct" "$remaining"

        if [[ "$converged_shards" -eq "$total_shards" ]]; then
            echo ""
            print_status_table "$data"
            ok "All ${total_shards} shard(s) converged."
            exit 0
        fi

        sleep "$poll_interval"
    done
}

# -- dispatch ----------------------------------------------------------------
case "$COMMAND" in
    create) cmd_create ;;
    status) cmd_status ;;
    delete) cmd_delete ;;
    wait)   cmd_wait   ;;
    help|--help|-h) usage; exit 0 ;;
    *) die "unknown command: '$COMMAND'. Use: create | status | delete | wait" ;;
esac
