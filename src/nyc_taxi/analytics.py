"""Capa 3a - Analítica OLAP con Spark SQL (Silver -> Gold).

Ejecuta cada consulta de `sql/gold/*.sql` sobre la vista `trips` (Silver), persiste el
resultado como tabla Gold en Parquet y exporta una copia CSV (son tablas pequeñas) para la
capa de servicio/visualización. Calcula además la matriz de correlaciones de los factores
que inciden en duración y costo (pregunta 2 del problema).
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .schema import PAYMENT_TYPES
from .utils import get_logger, write_json

log = get_logger("analytics")

CORR_VARS = ["trip_distance", "duration_min", "fare_amount", "total_amount", "tip_amount",
             "tolls_amount", "avg_speed_mph", "pickup_hour", "passenger_count", "cbd_congestion_fee"]


def register_views(spark: SparkSession, settings: Settings) -> None:
    trips = (spark.read.parquet(settings.silver_trips)
             .where((F.col("year") == settings.year) & F.col("month").isin(settings.months)))
    trips.createOrReplaceTempView("trips")
    spark.read.parquet(settings.bronze_zones).createOrReplaceTempView("zones")
    spark.createDataFrame(list(PAYMENT_TYPES.items()), "payment_type INT, payment_label STRING") \
        .createOrReplaceTempView("payment_types")


def correlations(spark: SparkSession) -> list[dict]:
    """Correlaciones de Pearson calculadas en una sola pasada distribuida."""
    trips = spark.table("trips")
    pairs = [(a, b) for i, a in enumerate(CORR_VARS) for b in CORR_VARS[i + 1:]]
    row = trips.agg(*[F.corr(a, b).alias(f"{a}__{b}") for a, b in pairs]).collect()[0].asDict()
    out = []
    for a, b in pairs:
        v = row[f"{a}__{b}"]
        out.append({"var_a": a, "var_b": b, "pearson": round(v, 4) if v is not None else None})
    return out


def run(spark: SparkSession, settings: Settings, export_dir: Path | None = None) -> dict:
    register_views(spark, settings)
    # Sin cache(): cachear la vista completa (43,7 M filas × 45 columnas) provocó un OutOfMemoryError
    # en un executor; cada consulta Gold lee pocas columnas y Parquet ya aplica column pruning.
    export_dir = export_dir or settings.outputs_dir / "gold"
    export_dir.mkdir(parents=True, exist_ok=True)

    summary: dict = {"tables": {}}
    for sql_file in sorted(settings.sql_dir.glob("gold/*.sql")):
        name = sql_file.stem.split("_", 1)[1]  # '01_demanda_hora_dia' -> 'demanda_hora_dia'
        df = spark.sql(sql_file.read_text(encoding="utf-8"))
        df.write.mode("overwrite").parquet(str(settings.gold_dir / name))
        pdf = spark.read.parquet(str(settings.gold_dir / name)).toPandas()
        pdf.to_csv(export_dir / f"{name}.csv", index=False)
        summary["tables"][name] = {"rows": len(pdf), "sql": sql_file.name}
        log.info("Gold '%s': %d filas", name, len(pdf))

    corr = correlations(spark)
    spark.createDataFrame(corr).write.mode("overwrite").parquet(str(settings.gold_dir / "correlaciones"))
    import pandas as pd
    pd.DataFrame(corr).to_csv(export_dir / "correlaciones.csv", index=False)
    summary["tables"]["correlaciones"] = {"rows": len(corr), "sql": "DataFrame API (F.corr)"}

    # KPIs globales del año (para el informe y el tablero)
    k = spark.sql("""
        SELECT COUNT(*) trips, ROUND(SUM(total_amount),2) revenue_usd,
               ROUND(AVG(trip_distance),3) avg_distance_mi, ROUND(AVG(duration_min),3) avg_duration_min,
               ROUND(AVG(avg_speed_mph),3) avg_speed_mph, ROUND(AVG(total_amount),3) avg_total_usd,
               ROUND(SUM(cbd_congestion_fee),2) cbd_fee_total_usd,
               ROUND(AVG(CAST(has_cbd_fee AS INT))*100,3) pct_trips_cbd_fee,
               ROUND(AVG(CAST(is_airport_trip AS INT))*100,3) pct_airport_trips,
               COUNT(DISTINCT pu_location_id) active_pickup_zones
        FROM trips""").collect()[0].asDict()
    summary["kpis"] = k
    write_json(settings.outputs_dir / "metrics" / "gold_summary.json", summary)
    return summary
