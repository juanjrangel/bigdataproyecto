-- Top 100 pares origen-destino por volumen de viajes.
SELECT pu_location_id, pu_zone, pu_borough, do_location_id, do_zone, do_borough,
       COUNT(*)                          AS trips,
       ROUND(AVG(trip_distance), 2)      AS avg_distance_mi,
       ROUND(AVG(duration_min), 2)       AS avg_duration_min,
       ROUND(PERCENTILE_APPROX(duration_min, 0.5), 2) AS p50_duration_min,
       ROUND(AVG(total_amount), 2)       AS avg_total_usd
FROM trips
GROUP BY pu_location_id, pu_zone, pu_borough, do_location_id, do_zone, do_borough
ORDER BY trips DESC
LIMIT 100
