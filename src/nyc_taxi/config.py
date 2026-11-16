"""Configuración centralizada del pipeline.

Todos los parámetros se leen de variables de entorno (ver `.env.example`), de modo que el
mismo código corre en el clúster Docker o en modo local (local[*]) sin cambios.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _parse_months(spec: str) -> list[int]:
    """Convierte '1-12', '1,2,3' o '3' en una lista ordenada de meses válidos."""
    months: set[int] = set()
    for part in spec.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            months.update(range(int(a), int(b) + 1))
        else:
            months.add(int(part))
    invalid = [m for m in months if m < 1 or m > 12]
    if invalid:
        raise ValueError(f"Meses inválidos en PIPELINE_MONTHS={spec!r}: {invalid}")
    return sorted(months)


@dataclass(frozen=True)
class Settings:
    # --- Ubicaciones -------------------------------------------------------
    project_dir: Path = Path(os.getenv("PROJECT_DIR", Path(__file__).resolve().parents[2]))
    lake_dir: Path = Path(os.getenv("LAKE_DIR", "/opt/lake"))

    # --- Alcance de los datos ---------------------------------------------
    year: int = int(os.getenv("PIPELINE_YEAR", "2025"))
    months: list[int] = field(default_factory=lambda: _parse_months(os.getenv("PIPELINE_MONTHS", "1-12")))
    tlc_base_url: str = os.getenv("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net")

    # --- Spark --------------------------------------------------------------
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    app_name: str = os.getenv("SPARK_APP_NAME", "nyc-taxi-bigdata")
    shuffle_partitions: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "144"))

    # --- Modelado -------------------------------------------------------------
    model_sample_fraction: float = float(os.getenv("MODEL_SAMPLE_FRACTION", "0.05"))
    model_seed: int = int(os.getenv("MODEL_SEED", "42"))
    gbt_max_iter: int = int(os.getenv("GBT_MAX_ITER", "40"))
    gbt_max_depth: int = int(os.getenv("GBT_MAX_DEPTH", "8"))

    # --- Evidencias ------------------------------------------------------------
    sample_rows: int = int(os.getenv("SAMPLE_ROWS", "50"))

    # Rutas derivadas -----------------------------------------------------------
    @property
    def landing_dir(self) -> Path:
        return self.project_dir / "data" / "raw"

    @property
    def landing_trips_dir(self) -> Path:
        return self.landing_dir / "yellow" / f"year={self.year}"

    @property
    def zone_lookup_csv(self) -> Path:
        return self.landing_dir / "reference" / "taxi_zone_lookup.csv"

    @property
    def zone_shapefile_zip(self) -> Path:
        return self.landing_dir / "reference" / "taxi_zones.zip"

    @property
    def bronze_trips(self) -> str:
        return str(self.lake_dir / "bronze" / "yellow_trips")

    @property
    def bronze_zones(self) -> str:
        return str(self.lake_dir / "bronze" / "taxi_zones")

    @property
    def silver_trips(self) -> str:
        return str(self.lake_dir / "silver" / "yellow_trips")

    @property
    def silver_quarantine(self) -> str:
        return str(self.lake_dir / "silver" / "quarantine")

    @property
    def gold_dir(self) -> Path:
        return self.lake_dir / "gold"

    @property
    def models_dir(self) -> Path:
        return self.lake_dir / "models"

    @property
    def outputs_dir(self) -> Path:
        # OUTPUTS_DIR permite aislar corridas de demostración sin tocar los resultados oficiales
        return Path(os.getenv("OUTPUTS_DIR", self.project_dir / "outputs"))

    @property
    def samples_dir(self) -> Path:
        return Path(os.getenv("SAMPLES_DIR", self.project_dir / "samples"))

    @property
    def sql_dir(self) -> Path:
        return self.project_dir / "sql"

    def month_file(self, month: int) -> Path:
        return self.landing_trips_dir / f"yellow_tripdata_{self.year}-{month:02d}.parquet"

    def month_url(self, month: int) -> str:
        return f"{self.tlc_base_url}/trip-data/yellow_tripdata_{self.year}-{month:02d}.parquet"


def get_settings() -> Settings:
    return Settings()
