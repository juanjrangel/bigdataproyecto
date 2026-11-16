-- Evolución mensual por borough de origen: volumen, ingresos y distancia.
SELECT month, pu_borough AS borough,
       COUNT(*)                        AS trips,
       ROUND(SUM(total_amount), 2)     AS revenue_usd,
       ROUND(AVG(trip_distance), 2)    AS avg_distance_mi,
       ROUND(AVG(duration_min), 2)     AS avg_duration_min,
       ROUND(AVG(avg_speed_mph), 2)    AS avg_speed_mph
FROM trips
GROUP BY month, pu_borough
ORDER BY month, trips DESC
