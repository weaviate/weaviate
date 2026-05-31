#!/usr/bin/env bash
# D2-PrePR P1 - Scenario 2: rolling upgrade base-main -> D2 in reader-ahead order.
#
# Reader-ahead-of-writer (synthesis §5.1): a node must READ V2 before any node
# WRITES V2. Sequence:
#   Stage 0: all-base-main, write V0, ingest.
#   Stage 1 (Release N): roll each node to D2 with write-new OFF (reads V2,
#            still writes V0). BM25 must stay correct at every step.
#   Stage 2 (Release N+1): enable write-new fleet-wide, re-ingest a doc to
#            force a V2 segment. BM25 (incl >65535 doc) must stay correct.
#
# Each node keeps its named volume across the container swap (true rolling
# upgrade: data persists, one node replaced at a time).
#
# Isolation: project d2prep1, subnet 10.98.0.0/16, ports 8490-8492.
set -uo pipefail
cd "$(dirname "$0")"
COMPOSE="docker compose -p d2prep1 -f docker-compose.mixed.yml"
BASE=weaviate-test:base-main-preguard
D2=weaviate-test:d2-bm25-c75c4bbe
PORTS=(8490 8491 8492)

wait_ready() { # port
  for i in $(seq 1 30); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$1/v1/.well-known/ready 2>/dev/null)" = "200" ] && return 0
    sleep 3
  done
  echo "TIMEOUT waiting for port $1"; return 1
}

wait_all() { for p in "${PORTS[@]}"; do wait_ready "$p" || return 1; done; }

probe_all() { # label
  for i in 0 1 2; do
    echo "--- probe node$i ($1) ---"
    python3 driver.py probe --node http://localhost:$((8490+i)) --label "$1-node$i"
  done
}

# roll one node: stop+rm its container, recreate with new image/flag on SAME volume
roll_node() { # idx image writev2
  local idx=$1 img=$2 wv=$3
  echo "=== rolling node$idx -> image=$img write_v2=${wv:-OFF} ==="
  NODE0_IMAGE=$NODE0_IMAGE NODE1_IMAGE=$NODE1_IMAGE NODE2_IMAGE=$NODE2_IMAGE \
  NODE0_WRITE_V2=$NODE0_WRITE_V2 NODE1_WRITE_V2=$NODE1_WRITE_V2 NODE2_WRITE_V2=$NODE2_WRITE_V2 \
    $COMPOSE up -d --no-deps --force-recreate node$idx 2>&1 | tail -2
  wait_ready $((8490+idx)) || return 1
}

export NODE0_IMAGE=$BASE NODE1_IMAGE=$BASE NODE2_IMAGE=$BASE
export NODE0_WRITE_V2= NODE1_WRITE_V2= NODE2_WRITE_V2=

echo "############ STAGE 0: all base-main, write V0 ############"
$COMPOSE down -v 2>&1 | tail -1
$COMPOSE up -d 2>&1 | tail -2
wait_all || exit 1
python3 driver.py create --node http://localhost:8490 >/dev/null && echo "class created"
python3 driver.py ingest --node http://localhost:8490
python3 driver.py wait --node http://localhost:8490 http://localhost:8491 http://localhost:8492
probe_all "stage0-allbase-V0"

echo "############ STAGE 1: roll each node to D2, write-new OFF (Release N) ############"
# reader-ahead-of-writer: every node now can READ V2 but still WRITES V0
for idx in 0 1 2; do
  eval "export NODE${idx}_IMAGE=$D2"
  roll_node $idx $D2 "" || exit 1
  wait_all || exit 1
  probe_all "stage1-node${idx}-upgraded-readerOFF"
done

echo "############ STAGE 2: enable write-new fleet-wide (Release N+1) ############"
export NODE0_WRITE_V2=true NODE1_WRITE_V2=true NODE2_WRITE_V2=true
for idx in 0 1 2; do
  roll_node $idx $D2 true || exit 1
  wait_all || exit 1
done
# force a NEW V2 segment by ingesting another overflow-bearing doc
python3 - <<'PY'
import urllib.request, json
body=json.dumps({"class":"DocV2","properties":{"content":"freshly written under write-new "+("v2seg "*70001)+"newmark"}}).encode()
r=urllib.request.Request("http://localhost:8490/v1/objects",data=body,method="POST")
r.add_header("Content-Type","application/json")
print("post new doc:", urllib.request.urlopen(r,timeout=60).status)
PY
sleep 3
python3 driver.py wait --node http://localhost:8490 http://localhost:8491 http://localhost:8492
probe_all "stage2-writenew-ON"
echo "############ ROLLING UPGRADE COMPLETE ############"
