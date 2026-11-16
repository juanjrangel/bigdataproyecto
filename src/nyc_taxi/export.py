"""Capa 4 - Servicio: muestras por etapa y base analítica DuckDB.

* samples/: 50 registros de cada etapa (raw, bronze, silver, cuarentena, gold) en CSV,
  como exige la entrega para evidenciar cada transformación en el repositorio.
* outputs/serving/nyc_taxi_gold.duckdb: base embebida con las tablas Gold, consultable con
  SQL estándar desde un notebook o un tablero sin necesidad de levantar Spark.
"""
from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from .config import Settings
from .utils import get_logger

log = get_logger("export")


def export_samples(spark: SparkSession, settings: Settings) -> dict:
    out = settings.samples_dir
    out.mkdir(parents=True, exist_ok=True)
    n, seed = settings.sample_rows, settings.model_seed
    first = settings.months[0]
    month_filter = F.col("month") == first

    frames = {
        "01_raw_landing": spark.read.parquet(str(settings.month_file(first))),
        "02_bronze": spark.read.parquet(settings.bronze_trips).where(month_filter),
        "03_silver": spark.read.parquet(settings.silver_trips).where(month_filter),
    }
    written = {}
    for name, df in frames.items():
        pdf = df.sample(fraction=0.001, seed=seed).limit(n).toPandas()
        pdf.to_csv(out / f"{name}_sample.csv", index=False)
        written[name] = len(pdf)

    # Cuarentena: muestra estratificada por motivo de rechazo (hasta 6 por regla)
    q = spark.read.parquet(settings.silver_quarantine).where(month_filter)
    w = Window.partitionBy("dq_first_failure").orderBy(F.rand(seed))
    qpdf = (q.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") <= 6).drop("_rn")
             .limit(n).toPandas())
    qpdf.to_csv(out / "04_quarantine_sample.csv", index=False)
    written["04_quarantine"] = len(qpdf)

    # Gold: primeras filas de cada tabla
    gold_out = out / "05_gold"
    gold_out.mkdir(exist_ok=True)
    for csv in sorted((settings.outputs_dir / "gold").glob("*.csv")):
        import pandas as pd
        pd.read_csv(csv).head(n).to_csv(gold_out / csv.name, index=False)
    written["05_gold_tables"] = len(list(gold_out.glob("*.csv")))
    log.info("Muestras exportadas en %s: %s", out, written)
    return written


def build_duckdb(settings: Settings) -> str:
    import duckdb

    serving = settings.outputs_dir / "serving"
    serving.mkdir(parents=True, exist_ok=True)
    db_path = serving / "nyc_taxi_gold.duckdb"
    tmp = settings.lake_dir / "serving_tmp.duckdb"  # se crea en disco Linux y luego se copia
    tmp.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp))
    for table_dir in sorted(settings.gold_dir.iterdir()):
        if table_dir.is_dir():
            con.execute(f"CREATE OR REPLACE TABLE {table_dir.name} AS "
                        f"SELECT * FROM read_parquet('{table_dir}/*.parquet')")
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    con.close()
    shutil.copyfile(tmp, db_path)
    log.info("Base DuckDB de servicio con %d tablas: %s", len(tables), db_path)
    return str(db_path)


def lake_sizes(settings: Settings) -> dict:
    """Tamaño en disco (MB) y número de archivos por capa del data lake."""
    from .utils import write_json

    def size(path) -> dict:
        files = [p for p in Path(path).rglob("*.parquet")]
        return {"mb": round(sum(p.stat().st_size for p in files) / 2**20, 1), "files": len(files)}

    out = {
        "landing_raw": size(settings.landing_trips_dir),
        "bronze": size(settings.bronze_trips),
        "silver": size(settings.silver_trips),
        "quarantine": size(settings.silver_quarantine),
        "gold": size(settings.gold_dir),
        "models": {"mb": round(sum(p.stat().st_size for p in settings.models_dir.rglob("*") if p.is_file()) / 2**20, 1)},
    }
    write_json(settings.outputs_dir / "metrics" / "lake_sizes.json", out)
    log.info("Tamaños del data lake: %s", out)
    return out


def run(spark: SparkSession, settings: Settings) -> dict:
    return {"samples": export_samples(spark, settings), "duckdb": build_duckdb(settings),
            "lake_sizes": lake_sizes(settings)}
