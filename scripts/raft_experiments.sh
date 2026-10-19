#!/usr/bin/env bash
# Live Raft experiments on the three coordinator replicas: election, leader crash,
# a crash between committing a plan and publishing it, a partitioned leader, and
# a long-isolated follower rejoining (PreVote).
set -uo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
NET=ocds_default
NODES="coordinator1 coordinator2 coordinator3"
PROBE=raft-probe

docker rm -f "$PROBE" >/dev/null 2>&1
docker run -d --name "$PROBE" --network "$NET" --entrypoint sleep curlimages/curl:8.10.1 infinity >/dev/null
trap 'docker rm -f "$PROBE" >/dev/null 2>&1' EXIT

status() { # node → "role term leader commit last_round pending"
  docker exec "$PROBE" curl -s --max-time 1 "http://$1:2112/status" 2>/dev/null |
    python -X utf8 -I -c "
import json,sys
try:
    d=json.load(sys.stdin); r=d['raft']; s=d['schedule']
    print(r['role'], r['term'], r['leader'] or '-', r['commit_index'], s['last_round'], s['pending'])
except Exception:
    print('down - - - - -')"
}
table() { for n in $NODES; do printf '    %-13s %s\n' "$n" "$(status "$n")"; done; }
leader() {
  for n in $NODES; do set -- $(status "$n"); [ "$1" = leader ] && { echo "$n"; return; }; done
}
wait_leader() { # [excluded node] → "leader elapsed_ms"
  local start=$(date +%s%3N)
  while true; do
    for n in $NODES; do
      [ "$n" = "${1:-}" ] && continue
      set -- $(status "$n"); [ "$1" = leader ] && { echo "$n $(( $(date +%s%3N) - start ))"; return; }
    done
    sleep 0.1
  done
}
plan() { docker exec "$PROBE" curl -s -o /dev/null -w '%{http_code}' -X POST "http://$1:2112/plan${2:-}"; }
term_of() { set -- $(status "$1"); echo "$2"; }

echo "== 1. election on start"
docker compose up -d --no-build $NODES >/dev/null 2>&1
read L ms < <(wait_leader)
echo "  leader $L"; sleep 4; table

echo "== 2. the leader crashes"
docker compose kill -s KILL "$L" >/dev/null 2>&1
read L2 ms < <(wait_leader "$L")
echo "  $L killed; new leader $L2 after ${ms} ms"; sleep 2; table
echo "  forced round on the new leader: HTTP $(plan "$L2")"; sleep 4; table
docker compose start "$L" >/dev/null 2>&1; sleep 8
echo "  $L restarted and caught up as follower:"; table

echo "== 3. crash between committing the plan and publishing it"
L=$(leader)
echo "  leader $L: POST /plan?crash=1 -> HTTP $(plan "$L" '?crash=1')"
sleep 1
read L2 ms < <(wait_leader "$L")
echo "  new leader $L2 after ${ms} ms"; sleep 5
docker compose logs "$L2" --no-log-prefix --since 30s 2>/dev/null | grep -E "republishing|round committed" | python -X utf8 -I -c "
import json,sys
for l in sys.stdin:
    d=json.loads(l); print('   ', d['msg'], {k:d[k] for k in ('round','tasks','term') if k in d})"
table
docker compose up -d "$L" >/dev/null 2>&1; sleep 8

echo "== 4. the leader is partitioned from the others"
L=$(leader); T=$(term_of "$L")
C=$(docker compose ps -q "$L")
docker network disconnect "$NET" "$C"
read L2 ms < <(wait_leader "$L")
echo "  $L (term $T) cut off; majority elected $L2 after ${ms} ms"
sleep 4
docker compose logs "$L" --no-log-prefix --since 15s 2>/dev/null | grep -E "lost contact" | head -1 | python -X utf8 -I -c "import json,sys;[print('   ',L:=json.loads(l)['msg'],'term',json.loads(l).get('term')) for l in sys.stdin]"
echo "  forced round on $L2 while $L is cut off: HTTP $(plan "$L2")"; sleep 4
docker network connect "$NET" "$C"; sleep 6
echo "  partition healed:"; table

echo "== 5. a follower isolated for 40 s rejoins (PreVote)"
L=$(leader); T=$(term_of "$L")
F=$(for n in $NODES; do [ "$n" != "$L" ] && { echo "$n"; break; }; done)
C=$(docker compose ps -q "$F")
docker network disconnect "$NET" "$C"; sleep 40
docker network connect "$NET" "$C"; sleep 8
echo "  leader before: $L term $T; after: $(leader) term $(term_of "$(leader)")"
echo "  elections attempted by $F while isolated: $(docker compose logs "$F" --no-log-prefix --since 60s 2>/dev/null | grep -c '"election started"')"
table
