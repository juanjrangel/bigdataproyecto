"""Utilidades transversales: logging, sesión Spark, métricas de ejecución y E/S de reportes."""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import Settings

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(LOG_FORMAT, "%Y-%m-%d %H:%M:%S"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def add_file_log(path: Path) -> None:
    """Duplica los logs del paquete en un archivo (evidencia de ejecución)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(logging.Formatter(LOG_FORMAT, "%Y-%m-%d %H:%M:%S"))
    for name in ("pipeline", "ingest", "transform", "analytics", "model", "validate", "export",
                 "visualize", "benchmark"):
        get_logger(name).addHandler(fh)


def build_spark(settings: Settings, app_suffix: str = "", extra_conf: dict[str, str] | None = None):
    """Crea (o reutiliza) la SparkSession con la configuración del proyecto.

    La mayoría de parámetros de clúster (memoria de executors, event log, etc.) viven en
    `docker/spark-defaults.conf`; aquí se fijan solo los que dependen del pipeline.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(f"{settings.app_name}{('-' + app_suffix) if app_suffix else ''}")
        .master(settings.spark_master)
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Las marcas de tiempo TLC son hora local de NYC sin zona: trabajamos en UTC para
        # que Spark no desplace las horas al extraer hour()/dayofweek().
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))  # ERROR en la demo para reducir ruido
    return spark


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


class RunMetrics:
    """Registra tiempos y conteos por etapa en outputs/metrics/run_metrics.json."""

    def __init__(self, settings: Settings):
        self.path = settings.outputs_dir / "metrics" / "run_metrics.json"
        self.data: dict[str, Any] = read_json(self.path, default={}) or {}

    @contextmanager
    def stage(self, name: str, logger: logging.Logger) -> Iterator[dict[str, Any]]:
        info: dict[str, Any] = {"started_at": datetime.now(timezone.utc).isoformat()}
        logger.info("==> Iniciando etapa '%s'", name)
        t0 = time.perf_counter()
        try:
            yield info
            info["status"] = "ok"
        except Exception as exc:  # se registra y se relanza para que el pipeline falle visible
            info["status"] = "error"
            info["error"] = f"{type(exc).__name__}: {exc}"
            logger.exception("Etapa '%s' falló", name)
            raise
        finally:
            info["seconds"] = round(time.perf_counter() - t0, 2)
            self.data[name] = info
            write_json(self.path, self.data)
            logger.info("<== Etapa '%s' terminó en %.1f s (%s)", name, info["seconds"], info.get("status"))
