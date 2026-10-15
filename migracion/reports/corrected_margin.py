"""Monthly margin report with the corrections that reconcile it with Accounting.

Same structure and output contract as the legacy replica (legacy_margin.py); every change is
marked with a `-- CORRECCION n` comment so it can be traced to CONCILIACION.md:

1. Products missing in mapeo_sku are matched by product name instead of being dropped
   (POS `descripcion` = productos.nombre; web `sku_web` = slug of productos.nombre).
2. Sales with estado NULL are valid sales (only ANULADO is excluded).
3. Delivery (categoria 'servicio') is included: it is revenue for Accounting (its cost is 0).
4. USD orders on days without a published exchange rate use the last published rate.
5. Cost is the one in force on the sale date, not today's cost.
(6. Late changes such as voided receipts are already applied by the silver MERGE.)
"""
from pyspark.sql import DataFrame, SparkSession

from migracion.reports.legacy_margin import REPORT_COLUMNS, register_silver_views

SOURCE_TABLES = ["ventas_pos", "pedidos_web", "mapeo_sku", "productos", "costos_historico", "tipo_cambio"]

CORRECTED_LINES_SQL = """
WITH productos_slug AS (
    SELECT p.*,
           lower(regexp_replace(translate(rtrim(p.nombre), 'áéíóúÁÉÍÓÚñÑ', 'aeiouAEIOUnN'), ' +', '-')) AS slug
    FROM productos p
),
-- CORRECCION 4: tipo de cambio de cada dia = ultimo publicado (fines de semana y feriados no se publican).
tc_diario AS (
    SELECT d.fecha,
           last_value(tc.tc, true) OVER (ORDER BY d.fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS tc
    FROM (
        SELECT explode(sequence(
                   (SELECT min(fecha) FROM tipo_cambio),
                   (SELECT max(CAST(fecha_pedido AS DATE)) FROM pedidos_web))) AS fecha
    ) d
    LEFT JOIN tipo_cambio tc ON tc.fecha = d.fecha
),
lineas AS (
    -- 1. Tiendas: boletas y facturas suman; notas de credito restan.
    SELECT
        v.id_tienda,
        CAST(date_trunc('MONTH', v.fecha_emision) AS DATE) AS mes,
        CAST(v.fecha_emision AS DATE) AS fecha,
        p.sku_erp,
        p.categoria,
        CASE WHEN v.tipo_comprobante <> '07'
             THEN concat(v.tipo_comprobante, '-', v.serie, '-', CAST(v.numero AS STRING)) END AS ticket,
        CASE WHEN v.tipo_comprobante = '07' THEN -v.cantidad ELSE v.cantidad END AS unidades,
        CAST(CASE WHEN v.tipo_comprobante = '07' THEN -v.importe_total ELSE v.importe_total END
             / 1.18 AS DECIMAL(12,2)) AS venta_sin_igv
    FROM ventas_pos v
    LEFT JOIN mapeo_sku m ON upper(rtrim(m.sku_pos)) = upper(rtrim(v.sku))
    -- CORRECCION 1: si el SKU no esta en mapeo_sku, se ubica el producto por su nombre.
    LEFT JOIN productos_slug p
           ON CASE WHEN m.sku_erp IS NOT NULL THEN upper(rtrim(p.sku_erp)) = upper(rtrim(m.sku_erp))
                   ELSE upper(rtrim(p.nombre)) = upper(rtrim(v.descripcion)) END
    WHERE v.tipo_comprobante IN ('01', '03', '07')
      -- CORRECCION 2: estado NULL es una venta valida (antes `estado <> 'ANULADO'` la descartaba).
      AND coalesce(upper(v.estado), '') <> 'ANULADO'
      -- CORRECCION 3: el delivery ya no se excluye (antes `categoria <> 'SERVICIO'`).

    UNION ALL

    -- 2. Tienda web: se reporta como una tienda mas. Los pedidos en dolares se pasan a soles.
    SELECT
        'WEB',
        CAST(date_trunc('MONTH', w.fecha_pedido) AS DATE),
        CAST(w.fecha_pedido AS DATE),
        p.sku_erp,
        p.categoria,
        w.id_pedido,
        w.cantidad,
        CAST(CASE WHEN w.moneda = 'USD' THEN w.importe * tc.tc ELSE w.importe END
             / 1.18 AS DECIMAL(12,2))
    FROM pedidos_web w
    LEFT JOIN mapeo_sku m ON upper(rtrim(m.sku_web)) = upper(rtrim(w.sku_web))
    -- CORRECCION 1: si el sku_web no esta en mapeo_sku, se ubica el producto por su nombre.
    LEFT JOIN productos_slug p
           ON CASE WHEN m.sku_erp IS NOT NULL THEN upper(rtrim(p.sku_erp)) = upper(rtrim(m.sku_erp))
                   ELSE p.slug = lower(rtrim(w.sku_web)) END
    -- CORRECCION 4: tipo de cambio del dia o el ultimo publicado.
    LEFT JOIN tc_diario tc ON tc.fecha = CAST(w.fecha_pedido AS DATE)
    WHERE w.estado = 'PAGADO'
      -- CORRECCION 3: el delivery ya no se excluye.
)
-- 3. CORRECCION 5: costo vigente a la fecha de la venta (antes: el vigente hoy para todos los meses).
SELECT l.*, CAST(l.unidades * c.costo_unitario AS DECIMAL(14,4)) AS costo
FROM lineas l
LEFT JOIN costos_historico c
       ON upper(rtrim(c.sku_erp)) = upper(rtrim(l.sku_erp))
      AND l.fecha >= c.vigente_desde
      AND (c.vigente_hasta IS NULL OR l.fecha <= c.vigente_hasta)
"""

CORRECTED_MARGIN_SQL = """
-- 4. Reporte (sin cambios).
WITH agg AS (
    SELECT mes, id_tienda, categoria,
           CAST(SUM(unidades) AS INT)          AS unidades,
           CAST(COUNT(DISTINCT ticket) AS INT) AS tickets,
           SUM(venta_sin_igv)                  AS venta_neta,
           CAST(SUM(costo) AS DECIMAL(14,2))   AS costo
    FROM corrected_lines
    GROUP BY mes, id_tienda, categoria
)
SELECT mes, id_tienda, categoria, unidades, tickets,
       CAST(unidades div NULLIF(tickets, 0) AS INT)                            AS unidades_por_ticket,
       CAST(venta_neta AS DECIMAL(14,2))                                       AS venta_neta,
       costo,
       CAST(venta_neta - costo AS DECIMAL(14,2))                               AS margen,
       CAST(100.0 * (venta_neta - costo) / NULLIF(venta_neta, 0) AS DECIMAL(9,2)) AS margen_pct
FROM agg
"""


def build_corrected_lines(spark: SparkSession, silver_path: str) -> DataFrame:
    """Line-level detail (one row per receipt/order line) with corrected sale and cost."""
    register_silver_views(spark, silver_path, SOURCE_TABLES)
    lines = spark.sql(CORRECTED_LINES_SQL)
    lines.createOrReplaceTempView("corrected_lines")
    return lines


def build_corrected_margin(spark: SparkSession, silver_path: str) -> DataFrame:
    build_corrected_lines(spark, silver_path)
    return spark.sql(CORRECTED_MARGIN_SQL).select(*REPORT_COLUMNS)
