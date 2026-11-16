"""Orquestador del pipeline NYC Taxi (punto de entrada para spark-submit).

Uso:
    spark-submit jobs/run_pipeline.py                      # todas las etapas
    spark-submit jobs/run_pipeline.py --stages ingest transform
    PIPELINE_MONTHS=1 spark-submit jobs/run_pipeline.py   # modo demo (1 mes)
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi import analytics, export, ingest, model, transform, validate  # noqa: E402
from nyc_taxi.config import get_settings  # noqa: E402
from nyc_taxi.utils import RunMetrics, add_file_log, build_spark, get_logger  # noqa: E402

STAGES = ["download", "ingest", "transform", "analytics", "model", "validate", "export", "visualize"]
SPARK_STAGES = {"ingest", "transform", "analytics", "model", "validate", "export"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stages", nargs="+", choices=STAGES, default=STAGES,
                        help="Etapas a ejecutar (en el orden canónico).")
    args = parser.parse_args()
    stages = [s for s in STAGES if s in args.stages]

    settings = get_settings()
    log_file = settings.outputs_dir / "logs" / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"
    add_file_log(log_file)
    log = get_logger("pipeline")
    log.info("Pipeline NYC TLC Yellow %d | meses=%s | master=%s | etapas=%s",
             settings.year, settings.months, settings.spark_master, stages)

    metrics = RunMetrics(settings)
    spark = None
    if SPARK_STAGES & set(stages):
        spark = build_spark(settings)
        sc = spark.sparkContext
        log.info("SparkSession lista: Spark %s, app_id=%s, paralelismo por defecto=%d",
                 spark.version, sc.applicationId, sc.defaultParallelism)

    try:
        for stage in stages:
            with metrics.stage(stage, log) as info:
                if stage == "download":
                    info["files"] = ingest.download_all(settings)
                elif stage == "ingest":
                    m = ingest.run(spark, settings)
                    info.update(rows=m["bronze_rows_total"], bytes=m["source_bytes_total"])
                elif stage == "transform":
                    r = transform.run(spark, settings)
                    info.update(rows_in=r["total"], rows_out=r["silver_rows"], retention_pct=r["retention_pct"])
                elif stage == "analytics":
                    s = analytics.run(spark, settings)
                    info.update(tables=len(s["tables"]), kpis=s["kpis"])
                elif stage == "model":
                    r = model.run(spark, settings)
                    info.update({t: v["models"]["gbt"] for t, v in r["targets"].items()})
                elif stage == "validate":
                    r = validate.run(spark, settings)
                    info.update(passed=r["passed"], total=r["total"])
                    if r["passed"] != r["total"]:
                        log.warning("Hay verificaciones fallidas: revisar outputs/validation/")
                elif stage == "export":
                    info.update(export.run(spark, settings))
                elif stage == "visualize":
                    from nyc_taxi import visualize
                    info["figures"] = visualize.run(settings)
    finally:
        if spark is not None:
            spark.stop()
    log.info("Pipeline finalizado. Log: %s", log_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
