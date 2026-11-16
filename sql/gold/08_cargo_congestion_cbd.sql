-- Primer año del peaje de congestión de Manhattan (CBD, desde 5-ene-2025):
-- proporción de viajes que lo pagan y su relación con la velocidad, por mes y hora.
SELECT month, pickup_hour, is_weekend,
       COUNT(*)                                                    AS trips,
       ROUND(AVG(CAST(has_cbd_fee AS INT)) * 100, 2)               AS pct_with_cbd_fee,
       ROUND(AVG(CASE WHEN has_cbd_fee THEN cbd_congestion_fee END), 3) AS avg_cbd_fee_usd,
       ROUND(SUM(cbd_congestion_fee), 2)                           AS cbd_fee_collected_usd,
       ROUND(AVG(CASE WHEN has_cbd_fee THEN avg_speed_mph END), 2)     AS speed_with_fee_mph,
       ROUND(AVG(CASE WHEN NOT has_cbd_fee THEN avg_speed_mph END), 2) AS speed_without_fee_mph
FROM trips
GROUP BY month, pickup_hour, is_weekend
ORDER BY month, is_weekend, pickup_hour
