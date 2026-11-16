"""Capa 2 - Procesamiento distribuido (Bronze -> Silver).

* Reglas de calidad declarativas (quality.py) con conteo por regla y cuarentena de
  registros rechazados (con el motivo), en lugar de descartarlos silenciosamente.
* Eliminación de duplicados exactos.
* Imputación explícita de categóricas nulas (RatecodeID -> 99, passenger_count se conserva nulo).
* Feature engineering temporal y de negocio.
* Enriquecimiento geoespacial con broadcast join contra la tabla de zonas TLC.
"""
from __future__ import annotations

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .quality import apply_rules, build_rules, rule_violation_counts, with_derived_metrics
from .schema import AIRPORT_ZONE_IDS
from .utils import get_logger, write_json

log = get_logger("transform")

BUSINESS_KEY = [
    "vendor_id", "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id",
    "trip_distance", "fare_amount", "total_amount", "payment_type",
]


def time_band(hour_col: F.Column, weekend_col: F.Column) -> F.Column:
    """Franja horaria operativa (picos solo en días hábiles)."""
    return (F.when(hour_col.between(0, 5), "Madrugada (0-5)")
             .when(~weekend_col & hour_col.between(6, 9), "Pico mañana (6-9)")
             .when(hour_col.between(6, 15), "Día (6-15)")
             .when(~weekend_col & hour_col.between(16, 19), "Pico tarde (16-19)")
             .when(hour_col.between(16, 19), "Tarde (16-19)")
             .otherwise("Noche (20-23)"))


def add_features(df: DataFrame) -> DataFrame:
    hour = F.hour("pickup_datetime")
    dow = F.dayofweek("pickup_datetime")  # 1 = domingo ... 7 = sábado
    is_weekend = dow.isin(1, 7)
    return (df
        .withColumn("ratecode_id", F.coalesce(F.col("ratecode_id"), F.lit(99)))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", hour)
        .withColumn("pickup_dow", dow)
        .withColumn("is_weekend", is_weekend)
        .withColumn("time_band", time_band(hour, is_weekend))
        .withColumn("tip_pct", F.when(F.col("fare_amount") > 0,
                                      F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)))
        .withColumn("has_tolls", F.col("tolls_amount") > 0)
        .withColumn("has_cbd_fee", F.coalesce(F.col("cbd_congestion_fee"), F.lit(0.0)) > 0)
        .withColumn("is_airport_trip",
                    F.col("ratecode_id").isin(2, 3)
                    | F.col("pu_location_id").isin(*AIRPORT_ZONE_IDS)
                    | F.col("do_location_id").isin(*AIRPORT_ZONE_IDS)))


def enrich_with_zones(df: DataFrame, zones: DataFrame) -> DataFrame:
    """Broadcast join (tabla de 265 filas) para origen y destino: evita shuffle de 48 M filas."""
    def side(prefix: str):
        cols = [F.col("location_id").alias(f"{prefix}_location_id"), F.col("borough").alias(f"{prefix}_borough"),
                F.col("zone").alias(f"{prefix}_zone"), F.col("service_zone").alias(f"{prefix}_service_zone")]
        if "centroid_x_ft" in zones.columns:
            cols += [F.col("centroid_x_ft").alias(f"{prefix}_x_ft"), F.col("centroid_y_ft").alias(f"{prefix}_y_ft")]
        return zones.select(*cols)

    out = (df.join(F.broadcast(side("pu")), "pu_location_id", "left")
             .join(F.broadcast(side("do")), "do_location_id", "left")
             .withColumn("same_borough", F.col("pu_borough") == F.col("do_borough")))
    if "centroid_x_ft" in zones.columns:
        # Distancia euclidiana entre centroides (EPSG:2263 está en pies) -> millas
        out = out.withColumn("centroid_dist_mi", F.round(F.sqrt(
            F.pow(F.col("pu_x_ft") - F.col("do_x_ft"), 2) + F.pow(F.col("pu_y_ft") - F.col("do_y_ft"), 2)) / 5280.0, 3))
    return out


def run(spark: SparkSession, settings: Settings) -> dict:
    bronze = (spark.read.parquet(settings.bronze_trips)
              .where((F.col("year") == settings.year) & F.col("month").isin(settings.months)))
    zones = spark.read.parquet(settings.bronze_zones).cache()

    rules = build_rules(settings.year)
    # DISK_ONLY: el DataFrame marcado (48,7 M filas + arreglo de reglas) no cabe holgadamente en el heap
    # de los executors; con MEMORY_AND_DISK se produjo un OutOfMemoryError (ver informe, sección 6).
    flagged = apply_rules(with_derived_metrics(bronze), rules).persist(StorageLevel.DISK_ONLY)
    report = rule_violation_counts(flagged, rules)
    log.info("Registros evaluados: %s | válidos: %s", f"{report['total']:,}", f"{report['valid']:,}")

    # --- Cuarentena: registros rechazados con su motivo (auditables) ------------------
    (flagged.where(~F.col("dq_is_valid"))
        .withColumn("dq_failed_rules", F.concat_ws("|", "dq_failed_rules"))
        .drop("dq_is_valid")
        .write.mode("overwrite").partitionBy("year", "month").parquet(settings.silver_quarantine))

    # --- Registros válidos: deduplicación + features + geo -----------------------------
    valid = flagged.where("dq_is_valid").drop("dq_failed_rules", "dq_first_failure", "dq_is_valid")
    deduped = valid.dropDuplicates(BUSINESS_KEY)
    silver = enrich_with_zones(add_features(deduped), zones)

    (silver.repartition("year", "month")
        .write.mode("overwrite").partitionBy("year", "month").parquet(settings.silver_trips))

    silver_rows = spark.read.parquet(settings.silver_trips).where(F.col("month").isin(settings.months)).count()
    flagged.unpersist()

    report["duplicates_removed"] = report["valid"] - silver_rows
    report["silver_rows"] = silver_rows
    report["quarantine_rows"] = report["total"] - report["valid"]
    report["retention_pct"] = round(silver_rows / report["total"] * 100, 3) if report["total"] else 0.0
    log.info("Silver: %s filas (retención %.2f %%), duplicados eliminados: %s, cuarentena: %s",
             f"{silver_rows:,}", report["retention_pct"], f"{report['duplicates_removed']:,}",
             f"{report['quarantine_rows']:,}")
    write_json(settings.outputs_dir / "quality" / "dq_report.json", report)
    return report
