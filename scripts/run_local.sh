#!/usr/bin/env bash
# 本地等价编排：不依赖 Docker，直接以多进程方式启动
#   source :8080 ；cache1 :8081 ；cache2 :8082
# 两个 cache 进程共享同一个 SQLite 文件（data/cache.db，WAL 模式）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$ROOT/.venv/bin/python"
DATA="$ROOT/data"
LOG="$ROOT/data/logs"
mkdir -p "$LOG"

SOURCE_DB="$DATA/source.db" CACHE_DB="$DATA/cache.db"
export SOURCE_DB CACHE_DB

stop() {
  [[ -n "${SRC_PID:-}" ]] && kill "$SRC_PID" 2>/dev/null || true
  [[ -n "${C1_PID:-}" ]] && kill "$C1_PID" 2>/dev/null || true
  [[ -n "${C2_PID:-}" ]] && kill "$C2_PID" 2>/dev/null || true
}
trap stop EXIT

"$PY" -m uvicorn app.source:app --host 127.0.0.1 --port 8080 \
  >"$LOG/source.log" 2>&1 &
SRC_PID=$!

SOURCE_BASE="http://127.0.0.1:8080" INSTANCE_ID=cache1 \
"$PY" -m uvicorn app.cache:app --host 127.0.0.1 --port 8081 \
  >"$LOG/cache1.log" 2>&1 &
C1_PID=$!

SOURCE_BASE="http://127.0.0.1:8080" INSTANCE_ID=cache2 \
"$PY" -m uvicorn app.cache:app --host 127.0.0.1 --port 8082 \
  >"$LOG/cache2.log" 2>&1 &
C2_PID=$!

# 等待健康
for i in $(seq 1 50); do
  ok=0
  curl -sf http://127.0.0.1:8080/docs >/dev/null && ok=$((ok+1))
  curl -sf http://127.0.0.1:8081/health >/dev/null && ok=$((ok+1))
  curl -sf http://127.0.0.1:8082/health >/dev/null && ok=$((ok+1))
  [[ $ok -eq 3 ]] && break
  sleep 0.3
done

echo "source=http://127.0.0.1:8080  cache1=http://127.0.0.1:8081  cache2=http://127.0.0.1:8082"
echo "PIDs: source=$SRC_PID cache1=$C1_PID cache2=$C2_PID (logs under $LOG)"
wait
