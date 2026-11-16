"""Benchmark de procesamiento distribuido (evidencia para la pregunta 3 del problema).

Experimentos sobre la capa Silver/Bronze ya construida:
  1. Escalabilidad horizontal: mismo trabajo con 2, 4, 6 y 12 núcleos del clúster (spark.cores.max).
  2. Formato columnar: Parquet vs CSV (tamaño en disco y tiempo de una agregación) para un mes.
  3. Proyección de columnas (column pruning): leer 3 columnas vs todas.
  4. Huella en memoria de pandas (1 mes cargado en RAM) extrapolada al año.

Uso:  spark-submit jobs/benchmark.py
"""
from __future__ import annotations

import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import functions as F  # noqa: E402

from nyc_taxi.config import get_settings  # noqa: E402
from nyc_taxi.utils import build_spark, get_logger, write_json  # noqa: E402

log = get_logger("benchmark")


def dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def timed(fn, repeats: int = 2) -> float:
    """Mejor tiempo de `repeats` ejecuciones (la primera calienta la JVM/caché del SO)."""
    best = float("inf")
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return round(best, 2)


def workload(spark, silver: str):
    """Trabajo representativo: agregación OD con percentil + join broadcast + escritura noop."""
    df = spark.read.parquet(silver)
    (df.groupBy("pu_location_id", "do_location_id", "pickup_hour")
       .agg(F.count("*").alias("n"), F.avg("duration_min").alias("d"),
            F.percentile_approx("avg_speed_mph", 0.5).alias("p50"), F.sum("total_amount").alias("rev"))
       .write.format("noop").mode("overwrite").save())


def main() -> None:
    s = get_settings()
    results: dict = {"scalability": [], "formats": {}, "pruning": {}, "pandas": {}}

    # 1. Escalabilidad -------------------------------------------------------------
    for cores in (2, 4, 6, 12):
        spark = build_spark(s, f"bench-{cores}c", {"spark.cores.max": str(cores),
                                                   "spark.executor.cores": str(min(cores, 6)),
                                                   "spark.sql.shuffle.partitions": str(max(cores * 3, 8))})
        rows = spark.read.parquet(s.silver_trips).count()
        secs = timed(lambda: workload(spark, s.silver_trips))
        results["scalability"].append({"cores": cores, "seconds": secs, "rows": rows})
        log.info("Escalabilidad: %2d núcleos -> %.1f s (%s filas)", cores, secs, f"{rows:,}")
        spark.stop()
    base = results["scalability"][0]["seconds"]
    for r in results["scalability"]:
        r["speedup"] = round(base / r["seconds"], 2)
        r["efficiency"] = round(r["speedup"] / (r["cores"] / results["scalability"][0]["cores"]), 2)

    spark = build_spark(s, "bench-formats")
    # 2. Parquet vs CSV (un mes de Bronze) -----------------------------------------
    bench = s.lake_dir / "bench"
    shutil.rmtree(bench, ignore_errors=True)
    month = spark.read.parquet(s.bronze_trips).where(F.col("month") == s.months[0]).drop("year", "month")
    month.write.mode("overwrite").option("header", True).csv(str(bench / "csv"))
    month.write.mode("overwrite").parquet(str(bench / "parquet"))
    csv_df = spark.read.option("header", True).option("inferSchema", False).csv(str(bench / "csv"))
    pq_df = spark.read.parquet(str(bench / "parquet"))
    agg = lambda df: df.groupBy("pu_location_id").agg(F.avg(F.col("total_amount").cast("double"))) \
        .write.format("noop").mode("overwrite").save()
    results["formats"] = {
        "rows": month.count(),
        "csv_mb": round(dir_size(bench / "csv") / 2**20, 1),
        "parquet_mb": round(dir_size(bench / "parquet") / 2**20, 1),
        "csv_seconds": timed(lambda: agg(csv_df)),
        "parquet_seconds": timed(lambda: agg(pq_df)),
    }
    log.info("Formatos: %s", results["formats"])

    # 3. Column pruning sobre Silver completo -----------------------------------------
    silver = spark.read.parquet(s.silver_trips)
    results["pruning"] = {
        "all_columns_seconds": timed(lambda: silver.write.format("noop").mode("overwrite").save()),
        "three_columns_seconds": timed(lambda: silver.select("pu_location_id", "pickup_hour", "total_amount")
                                       .write.format("noop").mode("overwrite").save()),
        "n_columns": len(silver.columns),
    }
    log.info("Proyección de columnas: %s", results["pruning"])
    spark.stop()
    shutil.rmtree(bench, ignore_errors=True)

    # 4. pandas: memoria de un mes en RAM -----------------------------------------------
    import pandas as pd

    t0 = time.perf_counter()
    pdf = pd.read_parquet(s.month_file(s.months[0]))
    mem = pdf.memory_usage(deep=True).sum()
    results["pandas"] = {"rows_one_month": len(pdf), "ram_gb_one_month": round(mem / 2**30, 2),
                         "load_seconds": round(time.perf_counter() - t0, 1),
                         "ram_gb_year_estimated": round(mem / 2**30 * 48_722_602 / len(pdf), 1)}
    log.info("pandas: %s", results["pandas"])
    write_json(s.outputs_dir / "metrics" / "benchmark.json", results)
    plot(s, results)


def plot(s, results) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from nyc_taxi.visualize import CAT, INK2, _save

    sc = results["scalability"]
    cores = [r["cores"] for r in sc]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8))
    a1.bar([str(c) for c in cores], [r["seconds"] for r in sc], color=CAT[0], width=0.6)
    for i, r in enumerate(sc):
        a1.text(i, r["seconds"], f"{r['seconds']:.0f} s", ha="center", va="bottom", fontsize=9, color=INK2)
    a1.set_xlabel("Núcleos asignados en el clúster")
    a1.set_title("Tiempo del trabajo OD sobre Silver")
    a1.grid(axis="x", visible=False)
    a2.plot(cores, [r["speedup"] for r in sc], color=CAT[0], marker="o", label="Speedup medido")
    a2.plot(cores, [c / cores[0] for c in cores], color=CAT[1], linestyle="--", label="Ideal lineal")
    a2.set_xticks(cores)
    a2.set_xlabel("Núcleos asignados en el clúster")
    a2.set_title("Aceleración respecto a 2 núcleos")
    a2.legend(loc="upper left")
    fig.suptitle(f"Escalabilidad horizontal de Spark ({sc[0]['rows'] / 1e6:.1f} M viajes)".replace(".", ","),
                 x=0.01, ha="left", fontweight="bold")
    fig.tight_layout()
    _save(fig, s.outputs_dir / "figures", "fig14_escalabilidad")


if __name__ == "__main__":
    main()
