"""Capa 1 - Ingesta (Landing -> Bronze).

1. Descarga idempotente de los Parquet mensuales oficiales de la TLC (con reintentos y
   verificación de tamaño) y de las tablas de referencia de zonas.
2. Lectura distribuida de cada mes con Spark, normalización del esquema (nombres/tipos)
   SIN alterar valores, y escritura en Bronze particionado por year/month.
3. Conciliación de conteos: filas en metadatos Parquet de origen == filas en Bronze.
"""
from __future__ import annotations

import time
import urllib.request
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import Settings
from .schema import CANONICAL_COLUMNS, REQUIRED_SOURCE_COLUMNS
from .utils import get_logger, write_json

log = get_logger("ingest")
USER_AGENT = "Mozilla/5.0 (compatible; academic-bigdata-pipeline/1.0)"


# ----------------------------------------------------------------------------------
# Descarga
# ----------------------------------------------------------------------------------
def _remote_size(url: str) -> int | None:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        size = resp.headers.get("Content-Length")
        return int(size) if size else None


def download_file(url: str, dest: Path, retries: int = 3) -> dict:
    """Descarga `url` en `dest` si no existe o si el tamaño local difiere del remoto."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    remote = None
    try:
        remote = _remote_size(url)
    except Exception as exc:  # sin red: si el archivo ya está, se continúa
        log.warning("No se pudo consultar tamaño remoto de %s (%s)", url, exc)
    if dest.exists() and (remote is None or dest.stat().st_size == remote):
        return {"file": dest.name, "status": "cached", "bytes": dest.stat().st_size}

    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=300) as resp, open(tmp, "wb") as fh:
                while chunk := resp.read(1 << 20):
                    fh.write(chunk)
            if remote is not None and tmp.stat().st_size != remote:
                raise IOError(f"tamaño incompleto {tmp.stat().st_size} != {remote}")
            tmp.replace(dest)
            log.info("Descargado %s (%.1f MB)", dest.name, dest.stat().st_size / 2**20)
            return {"file": dest.name, "status": "downloaded", "bytes": dest.stat().st_size}
        except Exception as exc:
            log.warning("Intento %d/%d falló para %s: %s", attempt, retries, url, exc)
            time.sleep(5 * attempt)
    raise RuntimeError(f"No fue posible descargar {url} tras {retries} intentos")


def download_all(settings: Settings) -> list[dict]:
    results = [download_file(settings.month_url(m), settings.month_file(m)) for m in settings.months]
    results.append(download_file(f"{settings.tlc_base_url}/misc/taxi_zone_lookup.csv", settings.zone_lookup_csv))
    results.append(download_file(f"{settings.tlc_base_url}/misc/taxi_zones.zip", settings.zone_shapefile_zip))
    return results


# ----------------------------------------------------------------------------------
# Landing -> Bronze
# ----------------------------------------------------------------------------------
def source_row_count(path: Path) -> int:
    """Número de filas declarado en el footer Parquet (sin leer los datos)."""
    import pyarrow.parquet as pq

    return pq.read_metadata(path).num_rows


def normalize_month(df: DataFrame, year: int, month: int, source_file: str) -> tuple[DataFrame, list[str]]:
    """Proyecta un DataFrame mensual al esquema canónico. Devuelve el DF y las alertas de esquema."""
    present = {c.lower(): c for c in df.columns}
    missing_required = [c for c in REQUIRED_SOURCE_COLUMNS if c.lower() not in present]
    if missing_required:
        raise ValueError(f"{source_file}: faltan columnas obligatorias {missing_required}")

    warnings: list[str] = []
    cols = []
    for src, dst, dtype in CANONICAL_COLUMNS:
        real = present.get(src.lower())
        if real is None:
            warnings.append(f"columna opcional ausente '{src}' -> se completa con NULL")
            cols.append(F.lit(None).cast(dtype).alias(dst))
        else:
            if real != src:
                warnings.append(f"columna '{real}' renombrada a '{dst}' (diferencia de mayúsculas)")
            cols.append(F.col(real).cast(dtype).alias(dst))
    extra = sorted(set(present) - {s.lower() for s, _, _ in CANONICAL_COLUMNS})
    if extra:
        warnings.append(f"columnas no catalogadas ignoradas: {extra}")

    out = df.select(
        *cols,
        F.lit(source_file).alias("source_file"),
        F.lit(datetime.now(timezone.utc).isoformat()).cast(T.TimestampType()).alias("ingestion_ts"),
        F.lit(year).alias("year"),
        F.lit(month).alias("month"),
    )
    return out, warnings


def ingest_trips(spark: SparkSession, settings: Settings) -> dict:
    frames: list[DataFrame] = []
    manifest: dict = {"year": settings.year, "months": {}, "schema_warnings": {}}
    for m in settings.months:
        path = settings.month_file(m)
        if not path.exists():
            raise FileNotFoundError(f"No existe {path}. Ejecute primero la etapa 'download'.")
        n_src = source_row_count(path)
        raw = spark.read.parquet(str(path))
        df, warns = normalize_month(raw, settings.year, m, path.name)
        if warns:
            manifest["schema_warnings"][path.name] = warns
            for w in warns:
                log.warning("%s: %s", path.name, w)
        manifest["months"][f"{m:02d}"] = {"file": path.name, "bytes": path.stat().st_size, "source_rows": n_src}
        frames.append(df)

    bronze = reduce(lambda a, b: a.unionByName(b), frames)
    (bronze.repartition("year", "month")
        .write.mode("overwrite").partitionBy("year", "month").parquet(settings.bronze_trips))
    log.info("Bronze escrito en %s", settings.bronze_trips)

    # Conciliación de conteos origen vs Bronze (lectura de solo metadatos de partición)
    counts = (spark.read.parquet(settings.bronze_trips)
              .where(F.col("month").isin(settings.months))
              .groupBy("month").count().collect())
    bronze_counts = {int(r["month"]): int(r["count"]) for r in counts}
    ok = True
    for m in settings.months:
        info = manifest["months"][f"{m:02d}"]
        info["bronze_rows"] = bronze_counts.get(m, 0)
        info["match"] = info["bronze_rows"] == info["source_rows"]
        ok &= info["match"]
    manifest["source_rows_total"] = sum(v["source_rows"] for v in manifest["months"].values())
    manifest["bronze_rows_total"] = sum(v["bronze_rows"] for v in manifest["months"].values())
    manifest["source_bytes_total"] = sum(v["bytes"] for v in manifest["months"].values())
    manifest["counts_reconciled"] = ok
    if not ok:
        raise AssertionError(f"Conteos Bronze no coinciden con el origen: {manifest['months']}")
    log.info("Ingesta conciliada: %s filas en %d meses", f"{manifest['bronze_rows_total']:,}", len(settings.months))
    return manifest


def zone_centroids(shapefile_zip: Path) -> list[tuple[int, float, float, float]]:
    """Centroide (fórmula del polígono/shoelace) y área de cada zona del shapefile TLC.

    Coordenadas en EPSG:2263 (NY State Plane, pies). Las zonas con varios polígonos
    (islas) se combinan ponderando por área.
    """
    import io
    import zipfile

    import shapefile  # pyshp

    with zipfile.ZipFile(shapefile_zip) as z:
        names = z.namelist()
        get = lambda ext: io.BytesIO(z.read(next(n for n in names if n.lower().endswith(ext))))
        reader = shapefile.Reader(shp=get(".shp"), shx=get(".shx"), dbf=get(".dbf"))
        idx = [f[0] for f in reader.fields[1:]].index("LocationID")
        acc: dict[int, list[float]] = {}
        for rec, shp in zip(reader.records(), reader.shapes()):
            pts, parts = shp.points, list(shp.parts) + [len(shp.points)]
            a_sum = cx_sum = cy_sum = 0.0
            for a, b in zip(parts[:-1], parts[1:]):
                ring = pts[a:b]
                for (x0, y0), (x1, y1) in zip(ring, ring[1:] + ring[:1]):
                    cross = x0 * y1 - x1 * y0
                    a_sum += cross
                    cx_sum += (x0 + x1) * cross
                    cy_sum += (y0 + y1) * cross
            area = a_sum / 2.0
            if area == 0:
                continue
            cx, cy = cx_sum / (6 * area), cy_sum / (6 * area)
            w = abs(area)
            s = acc.setdefault(int(rec[idx]), [0.0, 0.0, 0.0])
            s[0] += cx * w; s[1] += cy * w; s[2] += w
    return [(loc, sx / w, sy / w, w / 27_878_400.0) for loc, (sx, sy, w) in acc.items()]  # área en mi²


def ingest_zones(spark: SparkSession, settings: Settings) -> int:
    schema = T.StructType([
        T.StructField("LocationID", T.IntegerType(), False),
        T.StructField("Borough", T.StringType(), True),
        T.StructField("Zone", T.StringType(), True),
        T.StructField("service_zone", T.StringType(), True),
    ])
    lookup = (spark.read.option("header", True).schema(schema).csv(str(settings.zone_lookup_csv))
              .select(F.col("LocationID").alias("location_id"), F.col("Borough").alias("borough"),
                      F.col("Zone").alias("zone"), F.col("service_zone")))
    cent = spark.createDataFrame(zone_centroids(settings.zone_shapefile_zip),
                                 "location_id INT, centroid_x_ft DOUBLE, centroid_y_ft DOUBLE, area_sq_mi DOUBLE")
    zones = lookup.join(cent, "location_id", "left")
    log.info("Centroides geoespaciales calculados para %d zonas del shapefile", cent.count())
    zones.coalesce(1).write.mode("overwrite").parquet(settings.bronze_zones)
    n = zones.count()
    log.info("Tabla de zonas en Bronze: %d registros", n)
    return n


def run(spark: SparkSession, settings: Settings) -> dict:
    manifest = ingest_trips(spark, settings)
    manifest["zones_rows"] = ingest_zones(spark, settings)
    write_json(settings.outputs_dir / "metrics" / "ingestion_manifest.json", manifest)
    return manifest
