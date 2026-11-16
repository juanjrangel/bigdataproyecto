# Reporte de validación

17/17 verificaciones superadas.

| # | Verificación | Resultado | Detalle |
|---|---|---|---|
| 1 | Conciliación origen vs Bronze | ✅ | origen=48,722,602 bronze=48,722,602 |
| 2 | Bronze evaluado completo | ✅ | evaluados=48,722,602 bronze=48,722,602 |
| 3 | Conservación Bronze = Silver + Cuarentena + Duplicados | ✅ | 43,688,645 + 5,033,957 + 0 = 48,722,602 |
| 4 | Cuarentena persistida | ✅ | filas en cuarentena=5,033,957 |
| 5 | Silver sin nulos en marcas de tiempo | ✅ | nulos=0 |
| 6 | Silver 100 % geolocalizado (join de zonas) | ✅ | borough nulos=0 |
| 7 | Silver dentro de los umbrales de calidad | ✅ | dur=[1.0, 180.0] dist=[0.01, 99.06] fare=[0.01, 500.0] vel_max=80.0 |
| 8 | Silver dentro del periodo | ✅ | 2025-01-01 00:00:00 -> 2025-12-31 23:59:57 |
| 9 | Gold demanda_hora_dia suma = Silver | ✅ | gold=43,688,645 silver=43,688,645 |
| 10 | Gold demanda_zona suma = Silver | ✅ | gold=43,688,645 silver=43,688,645 |
| 11 | Gold ingresos = Silver (tolerancia 0,001 %) | ✅ | gold=1,240,642,021.46 silver=1,240,642,021.46 dif=0.000000 % |
| 12 | DuckDB (motor independiente) total = Spark | ✅ | duckdb=48,722,602 spark=48,722,602 |
| 13 | DuckDB registros válidos ≈ Spark (dif. <= 0,001 %) | ✅ | duckdb=43,688,645 spark=43,688,645 diferencia=0 |
| 14 | Modelo GBT supera baseline (duration_min) | ✅ | RMSE gbt=5.7953 baseline=13.6437 R²=0.8196 |
| 15 | Modelo GBT supera regresión lineal (duration_min) | ✅ | RMSE gbt=5.7953 lineal=7.5089 |
| 16 | Modelo GBT supera baseline (fare_amount) | ✅ | RMSE gbt=4.4181 baseline=15.9333 R²=0.9231 |
| 17 | Modelo GBT supera regresión lineal (fare_amount) | ✅ | RMSE gbt=4.4181 lineal=6.0702 |
