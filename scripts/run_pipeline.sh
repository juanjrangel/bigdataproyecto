#!/usr/bin/env bash
# Levanta el clúster, ejecuta pruebas y el pipeline completo. Uso: ./scripts/run_pipeline.sh [meses]
#   ./scripts/run_pipeline.sh          -> año completo (PIPELINE_MONTHS=1-12)
#   ./scripts/run_pipeline.sh 1        -> modo demo (solo enero, ~5 min)
set -euo pipefail
cd "$(dirname "$0")/.."
MONTHS="${1:-1-12}"
[ -f .env ] || cp .env.example .env
docker compose up -d --build
echo ">> Esperando a que los workers se registren en el master..."
for i in $(seq 1 30); do
  n=$(docker compose exec -T spark-master python3 -c "import json,urllib.request;print(len(json.load(urllib.request.urlopen('http://localhost:8080/json/'))['workers']))" 2>/dev/null || echo 0)
  [ "$n" -ge 2 ] && break; sleep 2
done
echo ">> Workers activos: $n"
docker compose exec -T spark-client python3 -m pytest
docker compose exec -T -e PIPELINE_MONTHS="$MONTHS" spark-client spark-submit jobs/run_pipeline.py
echo ">> Listo. Spark Master UI: http://localhost:8080 | History Server: http://localhost:18080"
