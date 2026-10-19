-- EsSalud medicine purchases in the Gold warehouse.
-- Run: docker compose --profile gold exec -T -e PGPASSWORD=... pg1 psql -h haproxy -p 5001 -U postgres -f - < analysis/essalud_medicines.sql
-- Medicines = CUBSO segment 51; Petitorio status comes from gold/pharma.py.
-- Amounts are in soles as published; referential = estimated value of the call, adjudicado = awarded.

\pset footer off
\pset format aligned

-- The view gold.v_essalud_medicamentos is defined in services/gold/src/gold/sql/schema.sql.

\echo '== 1. Volume'
SELECT count(*) AS items, count(DISTINCT ocid) AS procesos, count(DISTINCT fk_item) AS productos,
       round(sum(monto_referencial)::numeric / 1e6, 1) AS referencial_mm,
       round(sum(monto_adjudicado)::numeric / 1e6, 1) AS adjudicado_mm,
       count(monto_adjudicado) AS items_adjudicados
  FROM gold.v_essalud_medicamentos;

\echo '== 2. Context: EsSalud medicines among all medicine purchases of the State'
SELECT CASE WHEN e.es_essalud THEN 'EsSalud' ELSE 'resto del Estado' END AS comprador,
       count(*) AS items, round(sum(f.monto_referencial)::numeric / 1e6, 1) AS referencial_mm
  FROM gold.fact_items f
  JOIN gold.dim_item i ON i.sk_item = f.fk_item
  JOIN gold.dim_entidad_compradora e ON e.sk_entidad = f.fk_entidad
 WHERE i.es_medicamento
 GROUP BY 1 ORDER BY 1;

\echo '== 3. By fiscal year'
SELECT anio_fiscal, count(*) AS items, round(sum(monto_referencial)::numeric / 1e6, 1) AS referencial_mm
  FROM gold.v_essalud_medicamentos GROUP BY 1 ORDER BY 1;

\echo '== 4. Petitorio status'
SELECT estado, count(*) AS items, round(sum(monto_referencial)::numeric / 1e6, 1) AS referencial_mm,
       round(100.0 * sum(monto_referencial)::numeric / sum(sum(monto_referencial)::numeric) OVER (), 1) AS pct_monto
  FROM gold.v_essalud_medicamentos GROUP BY 1 ORDER BY 3 DESC NULLS LAST;

\echo '== 5. Procurement method'
SELECT detalles, count(*) AS items, round(sum(monto_referencial)::numeric / 1e6, 1) AS referencial_mm,
       sum(CASE WHEN estado = 'fuera de petitorio' THEN 1 ELSE 0 END) AS fuera_petitorio
  FROM gold.v_essalud_medicamentos GROUP BY 1 ORDER BY 3 DESC NULLS LAST LIMIT 10;

\echo '== 6. Out-of-formulary purchases by referential amount'
SELECT left(descripcion, 60) AS descripcion, count(*) AS items, round(sum(monto_referencial)::numeric / 1e6, 2) AS referencial_mm
  FROM gold.v_essalud_medicamentos WHERE estado = 'fuera de petitorio'
 GROUP BY descripcion ORDER BY 3 DESC NULLS LAST LIMIT 10;

\echo '== 7. Direct contracting: in vs out of the Petitorio'
SELECT estado, count(*) AS items,
       round(100.0 * avg(CASE WHEN flag_contratacion_directa THEN 1 ELSE 0 END), 1) AS pct_directa
  FROM gold.v_essalud_medicamentos GROUP BY 1 ORDER BY 2 DESC;

\echo '== 8. Awarded vs referential (only items with both amounts)'
SELECT estado, count(*) AS items,
       round(100.0 * (1 - sum(monto_adjudicado)::numeric / nullif(sum(monto_referencial)::numeric, 0)), 1) AS ahorro_pct
  FROM gold.v_essalud_medicamentos WHERE monto_adjudicado > 0 AND monto_referencial > 0
 GROUP BY 1 ORDER BY 2 DESC;

\echo '== 9. Supplier concentration on EsSalud medicine awards (HHI on awarded amount)'
WITH s AS (
  SELECT coalesce(p.nombre, '(sin proveedor)') AS proveedor, sum(m.monto_adjudicado) AS monto
    FROM gold.v_essalud_medicamentos m LEFT JOIN gold.dim_proveedor p ON p.sk_proveedor = m.fk_proveedor
   WHERE m.monto_adjudicado > 0 GROUP BY 1
), t AS (SELECT sum(monto) AS total FROM s)
SELECT count(*) AS proveedores,
       round(sum(power(100.0 * monto / total, 2))::numeric, 0) AS hhi,
       round((100.0 * max(monto) / max(total))::numeric, 1) AS pct_mayor_proveedor
  FROM s, t;

\echo '== 10. Top suppliers by awarded amount'
SELECT left(coalesce(p.nombre, '(sin proveedor)'), 45) AS proveedor, count(*) AS items,
       round(sum(m.monto_adjudicado)::numeric / 1e6, 2) AS adjudicado_mm
  FROM gold.v_essalud_medicamentos m LEFT JOIN gold.dim_proveedor p ON p.sk_proveedor = m.fk_proveedor
 WHERE m.monto_adjudicado > 0
 GROUP BY 1 ORDER BY 3 DESC LIMIT 8;

\echo '== 11. Lead time call -> signature (days), only items with a signed contract'
SELECT estado, count(lead_time_dias) AS con_firma,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY lead_time_dias) AS mediana,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY lead_time_dias) AS p90
  FROM gold.v_essalud_medicamentos WHERE lead_time_dias BETWEEN 0 AND 730 GROUP BY 1 ORDER BY 2 DESC;

\echo '== 12. Amendments (adendas)'
SELECT count(*) AS items, sum(CASE WHEN flag_tiene_adenda THEN 1 ELSE 0 END) AS con_adenda FROM gold.v_essalud_medicamentos;
