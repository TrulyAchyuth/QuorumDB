#!/usr/bin/env bash
# Run a local QuorumDB cluster as separate OS processes (no Docker needed).
#
#   scripts/cluster.sh start [N]    build if needed and start N nodes (default 3): n1..nN
#                                   HTTP on 8081..808N, Raft/gRPC on 9101..910N
#   scripts/cluster.sh stop         stop all nodes gracefully (SIGTERM)
#   scripts/cluster.sh kill <id>    SIGKILL one node (a real process crash)
#   scripts/cluster.sh revive <id>  start a stopped/killed node again on its existing data
#   scripts/cluster.sh status       show each node's health
#   scripts/cluster.sh clean        stop and delete all node data and logs
#
# Environment: QUORUMDB_FSYNC=false to disable fsync (benchmarks only), JAVA_OPTS for JVM flags,
# QUORUMDB_JAR to run a specific server jar instead of building backend/build/libs/quorumdb-server.jar.
# The admin token for fault injection is generated once into .run/admin-token.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$ROOT/.run"
JAR="${QUORUMDB_JAR:-$ROOT/backend/build/libs/quorumdb-server.jar}"
mkdir -p "$RUN"

token() {
  if [[ ! -s "$RUN/admin-token" ]]; then
    (umask 077; openssl rand -hex 24 > "$RUN/admin-token")
  fi
  cat "$RUN/admin-token"
}

build() {
  if [[ -n "${QUORUMDB_JAR:-}" ]]; then # an explicitly chosen jar is used as is
    [[ -f "$JAR" ]] || { echo "QUORUMDB_JAR=$JAR does not exist" >&2; exit 1; }
    return
  fi
  if [[ ! -f "$JAR" || -n "$(find "$ROOT/core/src" "$ROOT/simulation/src" "$ROOT/backend/src" "$ROOT/proto" -newer "$JAR" -type f -print -quit)" ]]; then
    (cd "$ROOT" && ./gradlew -q :backend:bootJar)
  fi
}

peers_for() { # $1 = self index, $2 = N
  local out="" i
  for ((i = 1; i <= $2; i++)); do
    [[ $i -eq $1 ]] && continue
    out+="${out:+,}n$i=localhost:$((9100 + i))=http://localhost:$((8080 + i))"
  done
  echo "$out"
}

start_node() { # $1 = index, $2 = N
  local i=$1 n=$2 id="n$1"
  if [[ -f "$RUN/$id.pid" ]] && kill -0 "$(cat "$RUN/$id.pid")" 2>/dev/null; then
    echo "$id already running (pid $(cat "$RUN/$id.pid"))"; return
  fi
  nohup java ${JAVA_OPTS:--Xmx512m} -jar "$JAR" \
    --server.port=$((8080 + i)) \
    --quorumdb.node-id="$id" \
    --quorumdb.grpc-port=$((9100 + i)) \
    --quorumdb.data-dir="$RUN/data/$id" \
    --quorumdb.peers="$(peers_for "$i" "$n")" \
    --quorumdb.advertised-http="http://localhost:$((8080 + i))" \
    --quorumdb.fsync="${QUORUMDB_FSYNC:-true}" \
    --quorumdb.admin-token="$(token)" \
    > "$RUN/$id.log" 2>&1 &
  echo $! > "$RUN/$id.pid"
  echo "$n" > "$RUN/size"
  echo "started $id (pid $!) http://localhost:$((8080 + i))"
}

wait_ready() { # $1 = N
  local i deadline=$((SECONDS + 60))
  for ((i = 1; i <= $1; i++)); do
    until curl -sf "http://localhost:$((8080 + i))/api/v1/health" >/dev/null 2>&1 \
        || curl -s "http://localhost:$((8080 + i))/api/v1/health" 2>/dev/null | grep -q '"status"'; do
      if ((SECONDS > deadline)); then echo "n$i did not come up; see $RUN/n$i.log" >&2; return 1; fi
      sleep 0.3
    done
  done
}

case "${1:-}" in
  start)
    N="${2:-3}"
    build
    for ((i = 1; i <= N; i++)); do start_node "$i" "$N"; done
    wait_ready "$N"
    echo "cluster of $N is up. Admin token: .run/admin-token"
    ;;
  stop)
    for f in "$RUN"/n*.pid; do [[ -f "$f" ]] || continue; kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
    echo "stopped"
    ;;
  kill)
    id="${2:?node id required}"
    kill -9 "$(cat "$RUN/$id.pid")" && rm -f "$RUN/$id.pid" && echo "killed $id (SIGKILL)"
    ;;
  revive)
    id="${2:?node id required}"
    build
    start_node "${id#n}" "$(cat "$RUN/size")"
    ;;
  status)
    for f in "$RUN"/n*.log; do
      [[ -f "$f" ]] || continue
      id="$(basename "$f" .log)"; port=$((8080 + ${id#n}))
      printf "%s  " "$id"; curl -s --max-time 1 "http://localhost:$port/api/v1/health" || printf "unreachable"; echo
    done
    ;;
  clean)
    "$0" stop >/dev/null
    rm -rf "$RUN/data" "$RUN"/n*.log "$RUN/size"
    echo "cleaned"
    ;;
  *)
    sed -n '2,13p' "$0"; exit 1
    ;;
esac
