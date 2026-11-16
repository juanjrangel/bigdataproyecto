"""Pruebas unitarias de las transformaciones (se ejecutan con Spark local dentro del contenedor)."""
from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from nyc_taxi import quality as Q
from nyc_taxi.config import _parse_months
from nyc_taxi.ingest import normalize_month
from nyc_taxi.transform import add_features, enrich_with_zones


def trip(**kw):
    base = dict(vendor_id=2, pickup_datetime=datetime(2025, 3, 10, 8, 0), dropoff_datetime=datetime(2025, 3, 10, 8, 20),
                passenger_count=1, trip_distance=3.0, ratecode_id=1, pu_location_id=161, do_location_id=236,
                payment_type=1, fare_amount=18.0, tip_amount=4.0, tolls_amount=0.0, total_amount=27.0,
                cbd_congestion_fee=0.75)
    base.update(kw)
    return Row(**base)


def evaluate(spark, rows):
    df = spark.createDataFrame(rows)
    flagged = Q.apply_rules(Q.with_derived_metrics(df), Q.build_rules(2025))
    return [(r.dq_is_valid, r.dq_first_failure) for r in flagged.collect()]


def test_parse_months():
    assert _parse_months("1-3,5") == [1, 2, 3, 5]
    with pytest.raises(ValueError):
        _parse_months("0-13")


def test_viaje_valido(spark):
    assert evaluate(spark, [trip()]) == [(True, None)]


@pytest.mark.parametrize("override,rule", [
    (dict(pickup_datetime=datetime(2008, 1, 1, 8), dropoff_datetime=datetime(2008, 1, 1, 8, 20)), "dentro_del_periodo"),
    (dict(dropoff_datetime=datetime(2025, 3, 10, 7, 50)), "fin_posterior_inicio"),
    (dict(dropoff_datetime=datetime(2025, 3, 10, 8, 0, 30)), "duracion_valida"),
    (dict(dropoff_datetime=datetime(2025, 3, 10, 12, 0)), "duracion_valida"),
    (dict(trip_distance=0.0), "distancia_valida"),
    (dict(fare_amount=-12.0), "tarifa_valida"),
    (dict(pu_location_id=264), "zonas_validas"),
    (dict(trip_distance=60.0), "velocidad_plausible"),
    (dict(passenger_count=9), "pasajeros_validos"),
])
def test_reglas_rechazan(spark, override, rule):
    valid, first = evaluate(spark, [trip(**override)])[0]
    assert valid is False
    assert first == rule


def test_pasajeros_nulos_se_conservan(spark):
    rows = [trip(passenger_count=None)]
    df = spark.createDataFrame(rows, schema=spark.createDataFrame([trip()]).schema)
    flagged = Q.apply_rules(Q.with_derived_metrics(df), Q.build_rules(2025))
    assert flagged.first().dq_is_valid


def test_features_y_join_geo(spark):
    df = Q.with_derived_metrics(spark.createDataFrame([trip(ratecode_id=None)],
                                                      schema=spark.createDataFrame([trip()]).schema))
    zones = spark.createDataFrame([(161, "Manhattan", "Midtown Center", "Yellow Zone"),
                                   (236, "Manhattan", "Upper East Side North", "Yellow Zone")],
                                  "location_id INT, borough STRING, zone STRING, service_zone STRING")
    r = enrich_with_zones(add_features(df), zones).first()
    assert r.duration_min == 20.0 and r.avg_speed_mph == 9.0
    assert r.ratecode_id == 99                     # imputación de nulos
    assert r.pickup_hour == 8 and r.time_band == "Pico mañana (6-9)"
    assert r.pu_borough == "Manhattan" and r.same_borough
    assert r.has_cbd_fee and not r.is_airport_trip


def test_normalizacion_esquema(spark):
    raw = spark.createDataFrame([(1, datetime(2025, 1, 1, 1), datetime(2025, 1, 1, 1, 10), 1.5, 10, 20, 9.0, 12.0, 1.75)],
                                "VendorID INT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, "
                                "trip_distance DOUBLE, PULocationID INT, DOLocationID INT, fare_amount DOUBLE, "
                                "total_amount DOUBLE, airport_fee DOUBLE")
    df, warns = normalize_month(raw, 2025, 1, "x.parquet")
    assert "airport_fee" in df.columns and "cbd_congestion_fee" in df.columns
    assert df.first().airport_fee == 1.75
    assert any("ausente" in w for w in warns)


def test_normalizacion_falla_sin_columnas_obligatorias(spark):
    raw = spark.createDataFrame([(1,)], "VendorID INT")
    with pytest.raises(ValueError):
        normalize_month(raw, 2025, 1, "x.parquet")
