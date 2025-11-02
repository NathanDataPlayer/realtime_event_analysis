#!/bin/bash
set -euo pipefail

HOST="${STARROCKS_HOST:-starrocks-fe}"
PORT="${STARROCKS_PORT:-9030}"

echo "[init] Waiting for StarRocks FE at ${HOST}:${PORT}..."
for i in {1..60}; do
  if mysql -h "$HOST" -P "$PORT" -uroot -e "SHOW FRONTENDS;" >/dev/null 2>&1; then
    echo "[init] StarRocks FE is ready."
    break
  fi
  echo "[init] Not ready yet, retry $i..."
  sleep 5
done

echo "[init] Adding BE node to FE..."
mysql -h "$HOST" -P "$PORT" -uroot -e "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';" || true
sleep 10
mysql -h "$HOST" -P "$PORT" -uroot -e "SHOW BACKENDS;"

echo "[init] Applying database and tables..."
mysql -h "$HOST" -P "$PORT" -uroot < /init/00_create_db.sql
mysql -h "$HOST" -P "$PORT" -uroot -D eventdb < /init/01_create_tables.sql

echo "[init] Creating materialized views..."
mysql -h "$HOST" -P "$PORT" -uroot -D eventdb < /init/02_create_mv.sql

echo "[init] Creating routine load jobs..."
mysql -h "$HOST" -P "$PORT" -uroot -D eventdb < /init/03_create_routine_load.sql

echo "[init] Initialization complete."