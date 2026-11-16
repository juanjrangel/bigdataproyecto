"""Esquema canónico y catálogos del Data Dictionary - Yellow Taxi Trip Records (TLC, 2025)."""
from __future__ import annotations

from pyspark.sql import types as T

# Esquema canónico de la capa Bronze. Las columnas se nombran en snake_case estable
# (p. ej. 'Airport_fee' -> 'airport_fee') y los tipos se fijan explícitamente para que
# un cambio de tipo entre meses (deriva de esquema) no rompa la unión de archivos.
CANONICAL_COLUMNS: list[tuple[str, str, T.DataType]] = [
    # (nombre_origen, nombre_destino, tipo)
    ("VendorID", "vendor_id", T.IntegerType()),
    ("tpep_pickup_datetime", "pickup_datetime", T.TimestampType()),
    ("tpep_dropoff_datetime", "dropoff_datetime", T.TimestampType()),
    ("passenger_count", "passenger_count", T.IntegerType()),
    ("trip_distance", "trip_distance", T.DoubleType()),
    ("RatecodeID", "ratecode_id", T.IntegerType()),
    ("store_and_fwd_flag", "store_and_fwd_flag", T.StringType()),
    ("PULocationID", "pu_location_id", T.IntegerType()),
    ("DOLocationID", "do_location_id", T.IntegerType()),
    ("payment_type", "payment_type", T.IntegerType()),
    ("fare_amount", "fare_amount", T.DoubleType()),
    ("extra", "extra", T.DoubleType()),
    ("mta_tax", "mta_tax", T.DoubleType()),
    ("tip_amount", "tip_amount", T.DoubleType()),
    ("tolls_amount", "tolls_amount", T.DoubleType()),
    ("improvement_surcharge", "improvement_surcharge", T.DoubleType()),
    ("total_amount", "total_amount", T.DoubleType()),
    ("congestion_surcharge", "congestion_surcharge", T.DoubleType()),
    ("Airport_fee", "airport_fee", T.DoubleType()),
    ("cbd_congestion_fee", "cbd_congestion_fee", T.DoubleType()),
]

REQUIRED_SOURCE_COLUMNS = {
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
    "PULocationID", "DOLocationID", "fare_amount", "total_amount",
}

VENDORS = {1: "Creative Mobile Technologies", 2: "Curb Mobility", 6: "Myle Technologies", 7: "Helix"}

PAYMENT_TYPES = {
    0: "Flex Fare", 1: "Tarjeta de crédito", 2: "Efectivo", 3: "Sin cargo",
    4: "Disputa", 5: "Desconocido", 6: "Viaje anulado",
}

RATE_CODES = {
    1: "Tarifa estándar", 2: "JFK", 3: "Newark", 4: "Nassau/Westchester",
    5: "Tarifa negociada", 6: "Viaje grupal", 99: "Nulo/Desconocido",
}

DAY_NAMES_ES = {1: "Dom", 2: "Lun", 3: "Mar", 4: "Mié", 5: "Jue", 6: "Vie", 7: "Sáb"}

# Zonas TLC con LocationID 264 ('Unknown') y 265 ('Outside of NYC') no tienen geometría.
MAX_VALID_ZONE_ID = 263
AIRPORT_ZONE_IDS = (1, 132, 138)  # Newark (EWR), JFK, LaGuardia
