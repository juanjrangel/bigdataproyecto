"""Pipeline Big Data de movilidad urbana - NYC TLC Yellow Taxi 2025.

Capas (arquitectura medallion):
    ingest     -> Bronze  (datos crudos normalizados, sin mutar valores)
    transform  -> Silver  (limpieza, reglas de calidad, features, join geoespacial)
    analytics  -> Gold    (agregaciones Spark SQL para demanda/ingresos/velocidad)
    model      -> Spark MLlib (predicción de duración y tarifa)
    serving    -> DuckDB + exportes CSV + figuras
"""

__version__ = "1.0.0"
