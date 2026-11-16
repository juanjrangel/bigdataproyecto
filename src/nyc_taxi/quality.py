"""Reglas de calidad de datos (declarativas) aplicadas en la transición Bronze -> Silver.

Cada regla define la condición que DEBE cumplir un registro válido. Los umbrales se
fijaron con un EDA sobre los 48,7 M de viajes de 2025 (percentiles 99,9 %) y con las
situaciones de calidad descritas en el informe de avance (sección 4.4).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schema import MAX_VALID_ZONE_ID

# Umbrales (documentados en el informe, sección "Caracterización de los datos")
MIN_DURATION_MIN = 1.0      # < 1 min: viajes cancelados/errores de taxímetro
MAX_DURATION_MIN = 180.0    # P99,9 = 114 min; > 3 h es inverosímil para un trayecto urbano
MAX_DISTANCE_MI = 100.0     # P99,9 = 30 mi; se tolera hasta 100 mi (aeropuertos/afueras)
MAX_FARE_USD = 500.0        # P99,9 = 148 USD
MAX_SPEED_MPH = 80.0        # P99,9 = 45 mph; > 80 mph indica distancia o tiempos corruptos
MAX_PASSENGERS = 6


@dataclass(frozen=True)
class Rule:
    name: str
    description: str
    condition: Column  # True => registro válido para esta regla


def with_derived_metrics(df: DataFrame) -> DataFrame:
    """Calcula duración (min) y velocidad media (mph), necesarias para varias reglas."""
    duration = (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0
    return (df.withColumn("duration_min", F.round(duration, 2))
              .withColumn("avg_speed_mph",
                          F.when(F.col("duration_min") > 0,
                                 F.round(F.col("trip_distance") / (F.col("duration_min") / 60.0), 2))))


def build_rules(year: int) -> list[Rule]:
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)
    return [
        Rule("timestamps_no_nulos", "Fecha/hora de inicio y fin presentes",
             F.col("pickup_datetime").isNotNull() & F.col("dropoff_datetime").isNotNull()),
        Rule("dentro_del_periodo", f"Inicio del viaje dentro de {year}",
             (F.col("pickup_datetime") >= F.lit(start)) & (F.col("pickup_datetime") < F.lit(end))),
        Rule("fin_posterior_inicio", "Hora de fin posterior a la de inicio",
             F.col("dropoff_datetime") > F.col("pickup_datetime")),
        Rule("duracion_valida", f"Duración entre {MIN_DURATION_MIN:g} y {MAX_DURATION_MIN:g} min",
             F.col("duration_min").between(MIN_DURATION_MIN, MAX_DURATION_MIN)),
        Rule("distancia_valida", f"Distancia > 0 y <= {MAX_DISTANCE_MI:g} millas",
             (F.col("trip_distance") > 0) & (F.col("trip_distance") <= MAX_DISTANCE_MI)),
        Rule("tarifa_valida", f"Tarifa base > 0 y <= {MAX_FARE_USD:g} USD y total > 0",
             (F.col("fare_amount") > 0) & (F.col("fare_amount") <= MAX_FARE_USD) & (F.col("total_amount") > 0)),
        Rule("zonas_validas", f"Zonas de origen/destino con geometría (1..{MAX_VALID_ZONE_ID})",
             F.col("pu_location_id").between(1, MAX_VALID_ZONE_ID)
             & F.col("do_location_id").between(1, MAX_VALID_ZONE_ID)),
        Rule("velocidad_plausible", f"Velocidad media <= {MAX_SPEED_MPH:g} mph",
             F.col("avg_speed_mph") <= MAX_SPEED_MPH),
        Rule("pasajeros_validos", f"Pasajeros nulo o entre 0 y {MAX_PASSENGERS}",
             F.col("passenger_count").isNull() | F.col("passenger_count").between(0, MAX_PASSENGERS)),
    ]


def apply_rules(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Añade `dq_failed_rules` (array con las reglas incumplidas) y `dq_first_failure`.

    Una condición que evalúa a NULL (p. ej. velocidad sin duración) se considera fallo.
    """
    failed = F.array_compact(F.array(*[
        F.when(~F.coalesce(r.condition, F.lit(False)), F.lit(r.name)) for r in rules
    ]))
    return (df.withColumn("dq_failed_rules", failed)
              .withColumn("dq_first_failure", F.element_at(F.col("dq_failed_rules"), 1))
              .withColumn("dq_is_valid", F.size("dq_failed_rules") == 0))


def rule_violation_counts(df_flagged: DataFrame, rules: list[Rule]) -> dict:
    """Conteo (en una sola pasada) de violaciones por regla y de primera falla."""
    exprs = [F.count(F.lit(1)).alias("total"),
             F.sum(F.col("dq_is_valid").cast("int")).alias("valid")]
    for r in rules:
        exprs.append(F.sum(F.array_contains("dq_failed_rules", r.name).cast("int")).alias(f"viol__{r.name}"))
        exprs.append(F.sum((F.col("dq_first_failure") == r.name).cast("int")).alias(f"first__{r.name}"))
    row = df_flagged.agg(*exprs).collect()[0].asDict()
    return {
        "total": int(row["total"]),
        "valid": int(row["valid"] or 0),
        "rules": [
            {"rule": r.name, "description": r.description,
             "violations": int(row[f"viol__{r.name}"] or 0),
             "first_failure": int(row[f"first__{r.name}"] or 0)}
            for r in rules
        ],
    }
