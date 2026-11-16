#!/usr/bin/env bash
# Arranca el rol del contenedor: master | worker | history | client | <comando arbitrario>
set -euo pipefail
mkdir -p /opt/lake/spark-events
ROLE="${1:-bash}"
case "$ROLE" in
  master)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
      --host spark-master --port 7077 --webui-port 8080 ;;
  worker)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
      --cores "${SPARK_WORKER_CORES:-4}" --memory "${SPARK_WORKER_MEMORY:-4g}" \
      --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" "${SPARK_MASTER_URL:-spark://spark-master:7077}" ;;
  history)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer ;;
  client)
    # Driver de larga vida: JupyterLab para exploración/demo; los jobs se lanzan con spark-submit
    exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root \
      --ServerApp.token="${JUPYTER_TOKEN:-bigdata}" --notebook-dir=/opt/project ;;
  *)
    exec "$@" ;;
esac
