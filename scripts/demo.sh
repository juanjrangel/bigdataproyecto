#!/usr/bin/env bash
# Demostración funcional aislada: procesa 1 mes con el clúster real, pero escribe en un
# data lake y unas salidas de demo separados, para no sobrescribir los resultados del año completo.
#   ./scripts/demo.sh            -> enero (PIPELINE_MONTHS=1)
#   ./scripts/demo.sh 3 ingest transform
set -euo pipefail
cd "$(dirname "$0")/.."
MONTHS="${1:-1}"; shift || true
STAGES=("$@")
docker compose exec -T \
  -e PIPELINE_MONTHS="$MONTHS" \
  -e LAKE_DIR=/opt/lake/demo \
  -e OUTPUTS_DIR=/opt/project/demo_outputs \
  -e SAMPLES_DIR=/opt/project/demo_outputs/samples \
  -e SPARK_APP_NAME=nyc-taxi-demo \
  -e SPARK_LOG_LEVEL=ERROR \
  spark-client spark-submit jobs/run_pipeline.py ${STAGES:+--stages "${STAGES[@]}"}
