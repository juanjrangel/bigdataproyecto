-- Franjas horarias operativas por borough (demanda, duración e ingreso).
SELECT time_band, pu_borough AS borough,
       COUNT(*)                         AS trips,
       ROUND(AVG(duration_min), 2)      AS avg_duration_min,
       ROUND(AVG(avg_speed_mph), 2)     AS avg_speed_mph,
       ROUND(AVG(total_amount), 2)      AS avg_total_usd,
       ROUND(SUM(total_amount), 2)      AS revenue_usd
FROM trips
GROUP BY time_band, pu_borough
ORDER BY trips DESC
