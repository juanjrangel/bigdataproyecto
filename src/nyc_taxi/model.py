"""Capa 3b - Modelado predictivo distribuido con Spark MLlib.

Objetivo específico 4 del avance: estimar (a) la duración del viaje y (b) la tarifa base
usando SOLO variables conocidas al inicio del viaje (sin fuga de información: no se usa
velocidad, propina ni total cobrado).

Para cada objetivo se comparan tres modelos sobre el mismo split 80/20:
  * Baseline ingenuo (predice la media del entrenamiento)
  * Regresión lineal (one-hot de zonas, hora y día)
  * Gradient-Boosted Trees (zonas representadas por sus centroides geográficos)

Decisión técnica: con las 263 zonas como variable categórica el GBT sobreajustaba
(R² 0,54 < regresión lineal). Se sustituyen por coordenadas de los centroides del shapefile
TLC y la distancia en línea recta entre ellos, que el árbol puede particionar espacialmente.
"""
from __future__ import annotations

import time

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .utils import get_logger, write_json

log = get_logger("model")

TARGETS = {
    "duration_min": "Duración del viaje (min)",
    "fare_amount": "Tarifa base (USD)",
}
NUMERIC = ["trip_distance", "pickup_hour", "pickup_dow", "month", "is_weekend_i", "is_airport_i",
           "same_borough_i", "ratecode_id"]
GEO = ["pu_x_ft", "pu_y_ft", "do_x_ft", "do_y_ft", "centroid_dist_mi"]
CATEGORICAL = ["pu_location_id", "do_location_id", "pu_borough", "do_borough"]   # para la regresión lineal
GBT_CATEGORICAL = ["pu_borough", "do_borough"]                                    # baja cardinalidad


def load_dataset(spark: SparkSession, settings: Settings) -> DataFrame:
    silver = (spark.read.parquet(settings.silver_trips)
              .where((F.col("year") == settings.year) & F.col("month").isin(settings.months)))
    return (silver.sample(withReplacement=False, fraction=settings.model_sample_fraction, seed=settings.model_seed)
            .select(*TARGETS, "trip_distance", "pickup_hour", "pickup_dow", "month", "ratecode_id",
                    "pu_location_id", "do_location_id", "pu_borough", "do_borough", "time_band",
                    *[F.coalesce(F.col(c), F.lit(0.0)).alias(c) for c in GEO],
                    F.col("is_weekend").cast("int").alias("is_weekend_i"),
                    F.col("is_airport_trip").cast("int").alias("is_airport_i"),
                    F.coalesce(F.col("same_borough"), F.lit(False)).cast("int").alias("same_borough_i"))
            .withColumn("pu_location_id", F.col("pu_location_id").cast("string"))
            .withColumn("do_location_id", F.col("do_location_id").cast("string")))


def _indexers() -> list[StringIndexer]:
    return [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL]


def linear_pipeline(target: str) -> Pipeline:
    ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL] + ["pickup_hour", "pickup_dow"],
                        outputCols=[f"{c}_ohe" for c in CATEGORICAL] + ["hour_ohe", "dow_ohe"],
                        handleInvalid="keep")
    assembler = VectorAssembler(
        inputCols=["trip_distance", "is_weekend_i", "is_airport_i", "same_borough_i", "month"]
                  + [f"{c}_ohe" for c in CATEGORICAL] + ["hour_ohe", "dow_ohe"],
        outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol=target, regParam=0.01, elasticNetParam=0.0, maxIter=50)
    return Pipeline(stages=[*_indexers(), ohe, assembler, lr])


def gbt_pipeline(target: str, settings: Settings) -> tuple[Pipeline, list[str]]:
    inputs = NUMERIC + GEO + [f"{c}_idx" for c in GBT_CATEGORICAL]
    assembler = VectorAssembler(inputCols=inputs, outputCol="features")
    gbt = GBTRegressor(featuresCol="features", labelCol=target, maxIter=settings.gbt_max_iter,
                       maxDepth=settings.gbt_max_depth, maxBins=64, stepSize=0.1, subsamplingRate=0.8,
                       seed=settings.model_seed)
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in GBT_CATEGORICAL]
    return Pipeline(stages=[*indexers, assembler, gbt]), inputs


def evaluate(pred: DataFrame, target: str) -> dict:
    ev = RegressionEvaluator(labelCol=target, predictionCol="prediction")
    return {m: round(ev.setMetricName(m).evaluate(pred), 4) for m in ("rmse", "mae", "r2")}


def run(spark: SparkSession, settings: Settings) -> dict:
    data = load_dataset(spark, settings).cache()
    train, test = data.randomSplit([0.8, 0.2], seed=settings.model_seed)
    train.cache(); test.cache()
    n_train, n_test = train.count(), test.count()
    log.info("Dataset de modelado: %s entrenamiento / %s prueba (fracción %.3f)",
             f"{n_train:,}", f"{n_test:,}", settings.model_sample_fraction)

    report: dict = {"sample_fraction": settings.model_sample_fraction, "train_rows": n_train,
                    "test_rows": n_test, "features_numeric": NUMERIC, "features_geo": GEO,
                    "features_categorical_lr": CATEGORICAL, "features_categorical_gbt": GBT_CATEGORICAL,
                    "gbt_params": {"maxIter": settings.gbt_max_iter, "maxDepth": settings.gbt_max_depth,
                                   "stepSize": 0.1, "subsamplingRate": 0.8, "maxBins": 64},
                    "targets": {}}
    out_dir = settings.outputs_dir / "model"
    out_dir.mkdir(parents=True, exist_ok=True)

    for target, label in TARGETS.items():
        res: dict = {"label": label, "models": {}}
        # 1) Baseline: media del entrenamiento
        mean_val = train.agg(F.avg(target)).collect()[0][0]
        res["models"]["baseline_media"] = evaluate(test.withColumn("prediction", F.lit(mean_val)), target)

        # 2) Regresión lineal
        t0 = time.perf_counter()
        lr_model = linear_pipeline(target).fit(train)
        res["models"]["regresion_lineal"] = evaluate(lr_model.transform(test), target)
        res["models"]["regresion_lineal"]["train_seconds"] = round(time.perf_counter() - t0, 1)

        # 3) Gradient-Boosted Trees
        t0 = time.perf_counter()
        pipe, inputs = gbt_pipeline(target, settings)
        gbt_model = pipe.fit(train)
        pred = gbt_model.transform(test).cache()
        res["models"]["gbt"] = evaluate(pred, target)
        res["models"]["gbt"]["train_seconds"] = round(time.perf_counter() - t0, 1)
        log.info("[%s] %s", target, res["models"])

        importances = gbt_model.stages[-1].featureImportances.toArray().tolist()
        res["feature_importance"] = sorted(
            [{"feature": f.removesuffix("_idx").removesuffix("_i"), "importance": round(v, 4)}
             for f, v in zip(inputs, importances)], key=lambda d: -d["importance"])

        # Error por franja horaria (¿dónde se equivoca el modelo?)
        by_band = (pred.groupBy("time_band")
                   .agg(F.count("*").alias("n"),
                        F.sqrt(F.avg(F.pow(F.col("prediction") - F.col(target), 2))).alias("rmse"),
                        F.avg(F.abs(F.col("prediction") - F.col(target))).alias("mae"))
                   .orderBy("time_band").toPandas())
        by_band.round(3).to_csv(out_dir / f"error_por_franja_{target}.csv", index=False)

        (pred.select(target, "prediction", "trip_distance", "pickup_hour", "pu_borough", "time_band")
             .sample(fraction=min(1.0, 6000 / max(n_test, 1)), seed=settings.model_seed)
             .limit(5000).toPandas().round(3)
             .to_csv(out_dir / f"predicciones_muestra_{target}.csv", index=False))

        gbt_model.write().overwrite().save(str(settings.models_dir / f"gbt_{target}"))
        pred.unpersist()
        report["targets"][target] = res

    write_json(settings.outputs_dir / "metrics" / "model_metrics.json", report)
    spark.catalog.clearCache()
    return report
