-- Viajes con origen o destino en aeropuertos (JFK, LaGuardia, Newark).
SELECT CASE WHEN pu_location_id = 132 OR do_location_id = 132 THEN 'JFK'
            WHEN pu_location_id = 138 OR do_location_id = 138 THEN 'LaGuardia'
            WHEN pu_location_id = 1   OR do_location_id = 1   THEN 'Newark (EWR)'
            ELSE 'Tarifa aeropuerto (otra zona)' END            AS airport,
       CASE WHEN pu_location_id IN (1, 132, 138) THEN 'Desde aeropuerto' ELSE 'Hacia aeropuerto' END AS direction,
       COUNT(*)                          AS trips,
       ROUND(AVG(trip_distance), 2)      AS avg_distance_mi,
       ROUND(AVG(duration_min), 2)       AS avg_duration_min,
       ROUND(AVG(total_amount), 2)       AS avg_total_usd,
       ROUND(AVG(tolls_amount), 2)       AS avg_tolls_usd,
       ROUND(SUM(total_amount), 2)       AS revenue_usd
FROM trips
WHERE is_airport_trip
GROUP BY 1, 2
ORDER BY trips DESC
