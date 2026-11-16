-- Demanda temporal: viajes, duración, velocidad e ingreso por día de la semana y hora.
SELECT pickup_dow,
       pickup_hour,
       COUNT(*)                         AS trips,
       ROUND(AVG(duration_min), 2)      AS avg_duration_min,
       ROUND(AVG(avg_speed_mph), 2)     AS avg_speed_mph,
       ROUND(AVG(total_amount), 2)      AS avg_total_usd,
       ROUND(SUM(total_amount), 2)      AS revenue_usd
FROM trips
GROUP BY pickup_dow, pickup_hour
ORDER BY pickup_dow, pickup_hour
