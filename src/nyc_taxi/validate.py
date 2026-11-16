"""Validación de extremo a extremo del pipeline.

Comprueba (y deja evidencia en outputs/validation/):
  1. Conciliación Landing -> Bronze (conteos del footer Parquet vs filas escritas).
  2. Conservación de registros Bronze = Silver + Cuarentena + Duplicados.
  3. Integridad de Silver: sin nulos en llaves, todas las zonas geolocalizadas, rangos válidos.
  4. Consistencia Silver -> Gold (totales de viajes e ingresos).
  5. Recalculo INDEPENDIENTE con DuckDB sobre los archivos crudos aplicando las mismas reglas
     (otro motor, otro código) -> debe coincidir con Spark.
  6. Calidad del modelo: GBT mejor que el baseline y que la regresión lineal.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import quality as Q
from .config import Settings
from .schema import MAX_VALID_ZONE_ID
from .utils import get_logger, read_json, write_json

log = get_logger("validate")


def _check(results: list, name: str, passed: bool, detail: str) -> None:
    results.append({"check": name, "passed": bool(passed), "detail": detail})
    (log.info if passed else log.error)("[%s] %s -> %s", "OK" if passed else "FALLA", name, detail)


def duckdb_independent_count(settings: Settings) -> dict:
    """Aplica las reglas de calidad en SQL de DuckDB directamente sobre los Parquet crudos."""
    import duckdb

    files = [str(settings.month_file(m)) for m in settings.months]
    y = settings.year
    con = duckdb.connect()
    con.execute(f"SET threads TO 4")
    sql = f"""
    WITH t AS (
      SELECT *,
        round(date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0, 2) AS dur
      FROM read_parquet({files!r})
    ), v AS (
      SELECT *, CASE WHEN dur > 0 THEN round(trip_distance / (dur / 60.0), 2) END AS spd FROM t
    )
    SELECT count(*) AS total,
      count(*) FILTER (WHERE
        tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL
        AND tpep_pickup_datetime >= TIMESTAMP '{y}-01-01' AND tpep_pickup_datetime < TIMESTAMP '{y + 1}-01-01'
        AND tpep_dropoff_datetime > tpep_pickup_datetime
        AND dur BETWEEN {Q.MIN_DURATION_MIN} AND {Q.MAX_DURATION_MIN}
        AND trip_distance > 0 AND trip_distance <= {Q.MAX_DISTANCE_MI}
        AND fare_amount > 0 AND fare_amount <= {Q.MAX_FARE_USD} AND total_amount > 0
        AND PULocationID BETWEEN 1 AND {MAX_VALID_ZONE_ID} AND DOLocationID BETWEEN 1 AND {MAX_VALID_ZONE_ID}
        AND spd <= {Q.MAX_SPEED_MPH}
        AND (passenger_count IS NULL OR passenger_count BETWEEN 0 AND {Q.MAX_PASSENGERS})
      ) AS valid
    FROM v"""
    total, valid = con.execute(sql).fetchone()
    return {"total": int(total), "valid": int(valid)}


def run(spark: SparkSession, settings: Settings) -> dict:
    results: list[dict] = []
    manifest = read_json(settings.outputs_dir / "metrics" / "ingestion_manifest.json", {})
    dq = read_json(settings.outputs_dir / "quality" / "dq_report.json", {})
    models = read_json(settings.outputs_dir / "metrics" / "model_metrics.json", {})

    # 1. Landing -> Bronze
    _check(results, "Conciliación origen vs Bronze", manifest.get("counts_reconciled", False),
           f"origen={manifest.get('source_rows_total'):,} bronze={manifest.get('bronze_rows_total'):,}")

    # 2. Conservación de registros
    total = dq.get("total", -1)
    accounted = dq.get("silver_rows", 0) + dq.get("quarantine_rows", 0) + dq.get("duplicates_removed", 0)
    _check(results, "Bronze evaluado completo", total == manifest.get("bronze_rows_total"),
           f"evaluados={total:,} bronze={manifest.get('bronze_rows_total'):,}")
    _check(results, "Conservación Bronze = Silver + Cuarentena + Duplicados", accounted == total,
           f"{dq.get('silver_rows'):,} + {dq.get('quarantine_rows'):,} + {dq.get('duplicates_removed'):,} = {accounted:,}")

    quarantine = spark.read.parquet(settings.silver_quarantine).where(F.col("month").isin(settings.months))
    q_rows = quarantine.count()
    _check(results, "Cuarentena persistida", q_rows == dq.get("quarantine_rows"),
           f"filas en cuarentena={q_rows:,}")

    # 3. Integridad de Silver
    silver = spark.read.parquet(settings.silver_trips).where(F.col("month").isin(settings.months))
    s = silver.agg(
        F.count("*").alias("n"),
        F.sum(F.col("pickup_datetime").isNull().cast("int")).alias("null_pickup"),
        F.sum(F.col("pu_borough").isNull().cast("int") + F.col("do_borough").isNull().cast("int")).alias("null_borough"),
        F.min("duration_min").alias("min_dur"), F.max("duration_min").alias("max_dur"),
        F.min("trip_distance").alias("min_dist"), F.max("trip_distance").alias("max_dist"),
        F.min("fare_amount").alias("min_fare"), F.max("fare_amount").alias("max_fare"),
        F.max("avg_speed_mph").alias("max_speed"),
        F.min("pickup_datetime").alias("min_ts"), F.max("pickup_datetime").alias("max_ts"),
        F.sum("total_amount").alias("revenue"),
    ).collect()[0].asDict()
    _check(results, "Silver sin nulos en marcas de tiempo", s["null_pickup"] == 0, f"nulos={s['null_pickup']}")
    _check(results, "Silver 100 % geolocalizado (join de zonas)", s["null_borough"] == 0,
           f"borough nulos={s['null_borough']}")
    in_range = (Q.MIN_DURATION_MIN <= s["min_dur"] and s["max_dur"] <= Q.MAX_DURATION_MIN
                and s["min_dist"] > 0 and s["max_dist"] <= Q.MAX_DISTANCE_MI
                and s["min_fare"] > 0 and s["max_fare"] <= Q.MAX_FARE_USD and s["max_speed"] <= Q.MAX_SPEED_MPH)
    _check(results, "Silver dentro de los umbrales de calidad", in_range,
           f"dur=[{s['min_dur']}, {s['max_dur']}] dist=[{s['min_dist']}, {s['max_dist']}] "
           f"fare=[{s['min_fare']}, {s['max_fare']}] vel_max={s['max_speed']}")
    _check(results, "Silver dentro del periodo", str(s["min_ts"]).startswith(str(settings.year))
           and str(s["max_ts"]).startswith(str(settings.year)), f"{s['min_ts']} -> {s['max_ts']}")

    # 4. Silver -> Gold
    gold_dir = settings.outputs_dir / "gold"
    g1 = pd.read_csv(gold_dir / "demanda_hora_dia.csv")
    g2 = pd.read_csv(gold_dir / "demanda_zona.csv")
    _check(results, "Gold demanda_hora_dia suma = Silver", int(g1.trips.sum()) == s["n"],
           f"gold={int(g1.trips.sum()):,} silver={s['n']:,}")
    _check(results, "Gold demanda_zona suma = Silver", int(g2.pickups.sum()) == s["n"],
           f"gold={int(g2.pickups.sum()):,} silver={s['n']:,}")
    rev_diff = abs(g1.revenue_usd.sum() - s["revenue"]) / s["revenue"] * 100
    _check(results, "Gold ingresos = Silver (tolerancia 0,001 %)", rev_diff < 0.001,
           f"gold={g1.revenue_usd.sum():,.2f} silver={s['revenue']:,.2f} dif={rev_diff:.6f} %")

    # 5. Recalculo independiente con DuckDB
    try:
        duck = duckdb_independent_count(settings)
        diff = abs(duck["valid"] - dq.get("valid", 0))
        _check(results, "DuckDB (motor independiente) total = Spark", duck["total"] == total,
               f"duckdb={duck['total']:,} spark={total:,}")
        _check(results, "DuckDB registros válidos ≈ Spark (dif. <= 0,001 %)", diff <= max(1, total * 1e-5),
               f"duckdb={duck['valid']:,} spark={dq.get('valid', 0):,} diferencia={diff:,}")
    except Exception as exc:  # si DuckDB no está disponible, la verificación queda registrada como fallida
        _check(results, "Recalculo independiente DuckDB", False, f"no ejecutado: {exc}")

    # 6. Modelos
    for target, res in (models.get("targets") or {}).items():
        m = res["models"]
        _check(results, f"Modelo GBT supera baseline ({target})", m["gbt"]["rmse"] < m["baseline_media"]["rmse"],
               f"RMSE gbt={m['gbt']['rmse']} baseline={m['baseline_media']['rmse']} R²={m['gbt']['r2']}")
        _check(results, f"Modelo GBT supera regresión lineal ({target})",
               m["gbt"]["rmse"] <= m["regresion_lineal"]["rmse"],
               f"RMSE gbt={m['gbt']['rmse']} lineal={m['regresion_lineal']['rmse']}")

    passed = sum(r["passed"] for r in results)
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "passed": passed,
              "total": len(results), "checks": results, "silver_stats": s}
    out = settings.outputs_dir / "validation"
    write_json(out / "validation_report.json", report)
    lines = ["| # | Verificación | Resultado | Detalle |", "|---|---|---|---|"]
    lines += [f"| {i} | {r['check']} | {'✅' if r['passed'] else '❌'} | {r['detail']} |"
              for i, r in enumerate(results, 1)]
    (out / "validation_report.md").write_text(
        f"# Reporte de validación\n\n{passed}/{len(results)} verificaciones superadas.\n\n" + "\n".join(lines) + "\n",
        encoding="utf-8")
    log.info("Validación: %d/%d verificaciones superadas", passed, len(results))
    return report
