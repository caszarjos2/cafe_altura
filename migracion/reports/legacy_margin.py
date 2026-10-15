"""Monthly margin report: faithful replica of rpt.usp_margen_mensual (legado/usp_margen_mensual.sql).

The SQL mirrors the stored procedure step by step, bugs included, so the output matches SQL Server.
Only two adaptations are needed because SQL Server runs with collation Modern_Spanish_CI_AS
(case-insensitive, trailing spaces ignored) and Spark compares strings exactly:
- SKU joins compare upper(rtrim(...)) on both sides (Cusco stores send 'ca-00024 ').
- The category filter compares upper(categoria) (the ERP stores 'servicio' in lowercase).
Decimal types and casts are the same as in SQL Server, so rounding matches.
"""
from pyspark.sql import DataFrame, SparkSession

SOURCE_TABLES = ["ventas_pos", "pedidos_web", "mapeo_sku", "productos", "costos_historico", "tipo_cambio"]

LEGACY_MARGIN_SQL = """
WITH lineas AS (
    -- 1. Tiendas: boletas y facturas suman; notas de credito restan.
    SELECT
        v.id_tienda,
        CAST(date_trunc('MONTH', v.fecha_emision) AS DATE) AS mes,
        m.sku_erp,
        p.categoria,
        CASE WHEN v.tipo_comprobante <> '07'
             THEN concat(v.tipo_comprobante, '-', v.serie, '-', CAST(v.numero AS STRING)) END AS ticket,
        CASE WHEN v.tipo_comprobante = '07' THEN -v.cantidad ELSE v.cantidad END AS unidades,
        CAST(CASE WHEN v.tipo_comprobante = '07' THEN -v.importe_total ELSE v.importe_total END
             / 1.18 AS DECIMAL(12,2)) AS venta_sin_igv
    FROM ventas_pos v
    LEFT JOIN mapeo_sku m ON upper(rtrim(m.sku_pos)) = upper(rtrim(v.sku))
    LEFT JOIN productos p ON upper(rtrim(p.sku_erp)) = upper(rtrim(m.sku_erp))
    WHERE v.tipo_comprobante IN ('01', '03', '07')
      AND v.estado <> 'ANULADO'
      AND upper(p.categoria) <> 'SERVICIO'

    UNION ALL

    -- 2. Tienda web: se reporta como una tienda mas. Los pedidos en dolares se pasan a soles.
    SELECT
        'WEB',
        CAST(date_trunc('MONTH', w.fecha_pedido) AS DATE),
        m.sku_erp,
        p.categoria,
        w.id_pedido,
        w.cantidad,
        CAST(CASE WHEN w.moneda = 'USD' THEN w.importe * tc.tc ELSE w.importe END
             / 1.18 AS DECIMAL(12,2))
    FROM pedidos_web w
    LEFT JOIN mapeo_sku m ON upper(rtrim(m.sku_web)) = upper(rtrim(w.sku_web))
    LEFT JOIN productos p ON upper(rtrim(p.sku_erp)) = upper(rtrim(m.sku_erp))
    LEFT JOIN tipo_cambio tc ON tc.fecha = CAST(w.fecha_pedido AS DATE)
    WHERE w.estado = 'PAGADO'
      AND upper(p.categoria) <> 'SERVICIO'
),
-- 3. Costo vigente de cada producto (el de hoy, aplicado a todos los meses).
con_costo AS (
    SELECT l.*, CAST(l.unidades * c.costo_unitario AS DECIMAL(14,4)) AS costo
    FROM lineas l
    LEFT JOIN costos_historico c
           ON upper(rtrim(c.sku_erp)) = upper(rtrim(l.sku_erp))
          AND c.vigente_hasta IS NULL
),
-- 4. Reporte.
agg AS (
    SELECT mes, id_tienda, categoria,
           CAST(SUM(unidades) AS INT)          AS unidades,
           CAST(COUNT(DISTINCT ticket) AS INT) AS tickets,
           SUM(venta_sin_igv)                  AS venta_neta,
           CAST(SUM(costo) AS DECIMAL(14,2))   AS costo
    FROM con_costo
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

REPORT_COLUMNS = [
    "mes", "id_tienda", "categoria", "unidades", "tickets", "unidades_por_ticket",
    "venta_neta", "costo", "margen", "margen_pct",
]


def register_silver_views(spark: SparkSession, silver_path: str, tables: list[str]) -> None:
    for table in tables:
        spark.read.format("delta").load(f"{silver_path}/{table}").createOrReplaceTempView(table)


def build_legacy_margin(spark: SparkSession, silver_path: str) -> DataFrame:
    register_silver_views(spark, silver_path, SOURCE_TABLES)
    return spark.sql(LEGACY_MARGIN_SQL).select(*REPORT_COLUMNS)
