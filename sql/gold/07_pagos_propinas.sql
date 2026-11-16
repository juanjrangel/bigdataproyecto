-- Formas de pago y comportamiento de propinas.
SELECT t.payment_type, p.payment_label,
       COUNT(*)                                AS trips,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS share_pct,
       ROUND(AVG(t.total_amount), 2)           AS avg_total_usd,
       ROUND(AVG(t.tip_amount), 2)             AS avg_tip_usd,
       ROUND(AVG(t.tip_pct), 2)                AS avg_tip_pct
FROM trips t LEFT JOIN payment_types p ON t.payment_type = p.payment_type
GROUP BY t.payment_type, p.payment_label
ORDER BY trips DESC
