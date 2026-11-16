-- Serie de tiempo diaria (demanda, ingreso, velocidad) para estacionalidad y festivos.
SELECT pickup_date,
       COUNT(*)                        AS trips,
       ROUND(SUM(total_amount), 2)     AS revenue_usd,
       ROUND(AVG(avg_speed_mph), 2)    AS avg_speed_mph,
       ROUND(AVG(duration_min), 2)     AS avg_duration_min
FROM trips
GROUP BY pickup_date
ORDER BY pickup_date
