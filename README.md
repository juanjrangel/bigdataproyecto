# Análisis masivo de movilidad urbana en la red de taxis de Nueva York con Big Data

Proyecto final de **Big Data** · Ingeniería en Ciencia de Datos · Universidad Industrial de Santander (UIS), 2026.

| Integrante | Código | Rol en el proyecto |
|---|---|---|
| Julián David Baena | 2243687 | Ingesta y almacenamiento (capa Bronze, descarga, conciliación de conteos) |
| Isabella Plata | 2243568 | Procesamiento y limpieza con PySpark (capa Silver, reglas de calidad, cuarentena, features) |
| Juan José Rangel | 2243554 | Analítica con Spark SQL y modelado con Spark MLlib (capa Gold, modelos GBT) |
| Juan Andrés Morera | 2243581 | Arquitectura e infraestructura Docker, validación, visualización y documentación |

Pipeline distribuido con **Apache Spark 3.5.5** sobre un **clúster standalone en Docker** (1 master + 2 workers)
que procesa los **48,7 millones de viajes de taxis amarillos de NYC del año 2025** (TLC Trip Record Data,
791 MB en Parquet) siguiendo una arquitectura *medallion* Bronze → Silver → Gold, con modelado predictivo en
Spark MLlib, validación cruzada con DuckDB y una capa de servicio con DuckDB + figuras.

> **Video de la demostración funcional (2 min 58 s):**
> <https://drive.google.com/file/d/15SK2NsP-0HaNgGtPmO7Kg-EXneAKm2YD/view?usp=sharing>
>
> El informe técnico y la presentación ejecutiva se entregaron en Moodle.

---

## 1. Arquitectura

```
 NYC TLC (CloudFront)                         Docker Compose · red nyc-taxi-net
 12 Parquet mensuales 2025 ─┐   ┌──────────────────────────────────────────────────────────────────┐
 taxi_zone_lookup.csv ──────┼──►│ spark-client (driver + JupyterLab) ── spark-submit run_pipeline   │
 taxi_zones.zip (shapefile) ┘   │        │ tareas                                                   │
                                │        ▼                                                          │
   data/raw (landing)           │ spark-master :8080 ──► spark-worker-1 (6 núcleos, 5 GB)           │
                                │                    └─► spark-worker-2 (6 núcleos, 5 GB)           │
                                │ spark-history :18080 (event log de cada aplicación)               │
                                └────────────┬─────────────────────────────────────────────────────┘
                                             ▼ volumen nyc-taxi-lake (/opt/lake)
   Capa 1 Bronze  : Parquet particionado year/month, esquema canónico, conciliación de conteos
   Capa 2 Silver  : 9 reglas de calidad + cuarentena, deduplicación, features, join geoespacial (broadcast)
   Capa 3 Gold    : 10 consultas Spark SQL + correlaciones | MLlib: regresión lineal y GBT (duración y tarifa)
   Capa 4 Servicio: DuckDB (outputs/serving), CSV Gold, 14 figuras, notebook de demostración
```

| Tecnología | Función |
|---|---|
| Apache Spark 3.5.5 (PySpark, Spark SQL, MLlib) | Procesamiento distribuido, ETL, agregaciones OLAP y entrenamiento de modelos |
| Docker + Docker Compose | Clúster reproducible: master, 2 workers, History Server y driver |
| Apache Parquet (Snappy) | Almacenamiento columnar particionado del data lake |
| DuckDB | Validación independiente sobre los datos crudos y capa de servicio SQL embebida |
| pandas / matplotlib / pyshp | Visualización y mapa coroplético a partir del shapefile oficial |
| pytest | Pruebas unitarias de reglas de calidad y transformaciones |

## 2. Estructura del repositorio

```
├── docker/                 Dockerfile, entrypoint por rol y spark-defaults.conf
├── docker-compose.yml      Clúster Spark standalone (master, 2 workers, history, client)
├── .env.example            Parámetros del pipeline (copiar a .env)
├── requirements.txt        Dependencias Python de la imagen
├── jobs/
│   ├── run_pipeline.py     Orquestador (etapas: download, ingest, transform, analytics, model, validate, export, visualize)
│   └── benchmark.py        Escalabilidad (2→12 núcleos), Parquet vs CSV, column pruning, memoria pandas
├── src/nyc_taxi/
│   ├── config.py           Configuración por variables de entorno
│   ├── schema.py           Esquema canónico y catálogos del Data Dictionary
│   ├── ingest.py           Capa 1: descarga, normalización de esquema, Bronze, centroides de zonas
│   ├── quality.py          Reglas de calidad declarativas y conteo de violaciones
│   ├── transform.py        Capa 2: cuarentena, deduplicación, features, join geoespacial → Silver
│   ├── analytics.py        Capa 3: ejecución de sql/gold/*.sql → Gold + correlaciones + KPIs
│   ├── model.py            Capa 3: Spark MLlib (baseline, regresión lineal, GBT)
│   ├── validate.py         17 verificaciones de extremo a extremo (incluye DuckDB independiente)
│   ├── export.py           Muestras por etapa y base DuckDB de servicio
│   └── visualize.py        Figuras PNG
├── sql/gold/               10 consultas Spark SQL de la capa Gold
├── tests/                  Pruebas unitarias (pytest + Spark local)
├── notebooks/demo_pipeline.ipynb   Consultas sobre el data lake: Bronze, Silver, Spark SQL, modelo GBT y DuckDB
├── scripts/                run_pipeline.sh / run_pipeline.ps1 (todo en uno) y demo.sh (modo demo de 1 mes)
├── samples/                Muestras de 50 registros: raw, bronze, silver y cuarentena
└── outputs/                Resultados del año completo 2025
    ├── gold/               Tablas Gold completas en CSV (resultados de la capa 3)
    ├── serving/            nyc_taxi_gold.duckdb (capa de servicio)
    ├── figures/            14 figuras de resultados
    ├── metrics/            Manifiesto de ingesta, tiempos por etapa, tamaños del lake, métricas de modelos, benchmark
    ├── quality/            Reporte de calidad (violaciones por regla)
    ├── validation/         Reporte de las 17 verificaciones
    └── evidence/           Capturas de Spark Master UI y History Server de la ejecución
```

Al ejecutar el pipeline también se generan, sin versionarse, `samples/05_gold/`, `outputs/model/`,
`outputs/experiments/` y `outputs/logs/`.

## 3. Requisitos

* Docker Engine 24+ con Docker Compose v2 (Docker Desktop en Windows/macOS o Docker nativo en Linux/WSL2).
* ≥ 14 GB de RAM asignables a Docker y ~4 GB de disco libre (datos + data lake).
* Conexión a internet para la primera descarga (~792 MB).

No se requiere instalar Java, Spark ni Python en el equipo: todo corre dentro de la imagen
`nyc-taxi-spark:1.0` (base oficial `apache/spark:3.5.5-scala2.12-java17-python3-ubuntu`).

## 4. Ejecución

```bash
git clone https://github.com/juanjrangel/bigdataproyecto.git
cd bigdataproyecto
cp .env.example .env                 # parámetros (meses, recursos de workers, muestra del modelo)
docker compose up -d --build         # levanta master, 2 workers, history server y driver
docker compose exec spark-client python3 -m pytest                   # 15 pruebas unitarias
docker compose exec spark-client spark-submit jobs/run_pipeline.py   # pipeline completo (~25 min, 12 meses)
docker compose exec spark-client spark-submit jobs/benchmark.py      # opcional: experimentos de escalabilidad
```

* **Modo demo aislado (1 mes, ~4 min):** `./scripts/demo.sh 1` — usa el clúster real pero escribe en `/opt/lake/demo` y
  `demo_outputs/`, sin sobrescribir los resultados del año completo.
* **Video de la demostración (2 min 58 s):** [Google Drive](https://drive.google.com/file/d/15SK2NsP-0HaNgGtPmO7Kg-EXneAKm2YD/view?usp=sharing)
  — ejecución real de este modo demo grabada en la terminal, con capturas de la Spark UI.
* **Etapas sueltas:** `... spark-submit jobs/run_pipeline.py --stages analytics validate visualize`
* **Script todo-en-uno:** `./scripts/run_pipeline.sh [meses]` (bash) o `.\scripts\run_pipeline.ps1 -Months 1-12` (PowerShell).

Interfaces web (solo en `localhost`):

| Servicio | URL |
|---|---|
| Spark Master UI (workers y aplicaciones) | <http://localhost:8080> |
| Spark UI de la aplicación en curso | <http://localhost:4040> |
| Spark History Server | <http://localhost:18080> |
| JupyterLab (notebook de demo, token en `.env`) | <http://localhost:8888> |

Detener: `docker compose down` (el data lake persiste en el volumen `nyc-taxi-lake`; `docker compose down -v` lo elimina).

## 5. Datos

* **Fuente:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) —
  Yellow Taxi Trip Records, enero–diciembre 2025 (12 archivos Parquet, 791 MB, 48.722.602 viajes, 20 columnas).
* **Referencia geoespacial:** Taxi Zone Lookup Table (CSV, 265 zonas) y Taxi Zone Shapefile (263 polígonos, EPSG:2263).
* Los archivos originales **no** se versionan (`.gitignore`); la etapa `download` los obtiene de forma idempotente
  desde `https://d37ci6vzurychx.cloudfront.net/trip-data/`. El volumen Bronze/Silver/Gold vive en el volumen Docker.
* En el repositorio se incluyen muestras de 50 registros de cada etapa en [`samples/`](samples/) (raw, Bronze,
  Silver y cuarentena); la capa Gold está completa en [`outputs/gold/`](outputs/gold/).

## 6. Calidad y validación

* 9 reglas de calidad declarativas (`src/nyc_taxi/quality.py`); los registros rechazados se guardan en
  `silver/quarantine` con el motivo (`dq_failed_rules`), no se descartan en silencio.
* 17 verificaciones automáticas (`outputs/validation/validation_report.md`): conciliación origen↔Bronze,
  conservación de registros, integridad de Silver, consistencia Silver↔Gold, **recálculo independiente con DuckDB**
  y desempeño de los modelos frente a baselines.
* Parámetros sensibles: no hay credenciales en el código; el token de JupyterLab se toma de `.env` (no versionado)
  y todos los puertos se publican solo en `127.0.0.1`.
