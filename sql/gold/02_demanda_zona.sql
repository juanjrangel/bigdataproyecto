-- Métricas por zona de recogida (base para el mapa coroplético de demanda).
WITH pu AS (
  SELECT pu_location_id AS location_id,
         COUNT(*)                                   AS pickups,
         ROUND(SUM(total_amount), 2)                AS revenue_usd,
         ROUND(AVG(fare_amount), 2)                 AS avg_fare_usd,
         ROUND(AVG(trip_distance), 2)               AS avg_distance_mi,
         ROUND(AVG(duration_min), 2)                AS avg_duration_min,
         ROUND(AVG(avg_speed_mph), 2)               AS avg_speed_mph,
         ROUND(AVG(CASE WHEN payment_type = 1 THEN tip_pct END), 2) AS avg_tip_pct_card
  FROM trips GROUP BY pu_location_id
), dof AS (
  SELECT do_location_id AS location_id, COUNT(*) AS dropoffs
  FROM trips GROUP BY do_location_id
)
SELECT z.location_id, z.borough, z.zone, z.service_zone,
       COALESCE(pu.pickups, 0)  AS pickups,
       COALESCE(dof.dropoffs, 0) AS dropoffs,
       pu.revenue_usd, pu.avg_fare_usd, pu.avg_distance_mi, pu.avg_duration_min,
       pu.avg_speed_mph, pu.avg_tip_pct_card,
       ROUND(COALESCE(pu.pickups, 0) * 100.0 / SUM(COALESCE(pu.pickups, 0)) OVER (), 4) AS pickup_share_pct
FROM zones z
LEFT JOIN pu  ON z.location_id = pu.location_id
LEFT JOIN dof ON z.location_id = dof.location_id
WHERE z.location_id <= 263
ORDER BY pickups DESC
