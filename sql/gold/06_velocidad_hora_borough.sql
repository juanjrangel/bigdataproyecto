-- Curvas de tráfico: velocidad media y percentiles de duración por hora, borough y tipo de día.
SELECT pu_borough AS borough, is_weekend, pickup_hour,
       COUNT(*)                                        AS trips,
       ROUND(AVG(avg_speed_mph), 2)                    AS avg_speed_mph,
       ROUND(PERCENTILE_APPROX(avg_speed_mph, 0.5), 2) AS p50_speed_mph,
       ROUND(AVG(duration_min / trip_distance), 2)     AS min_per_mile
FROM trips
WHERE pu_borough IN ('Manhattan', 'Brooklyn', 'Queens', 'Bronx')
GROUP BY pu_borough, is_weekend, pickup_hour
ORDER BY borough, is_weekend, pickup_hour
