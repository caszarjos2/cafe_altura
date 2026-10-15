/* =====================================================================
   rpt.usp_margen_mensual — reporte de margen mensual
   ---------------------------------------------------------------------
   Lo ejecuta el job nocturno de SQL Agent, después de la carga del SSIS.
   Recalcula el reporte completo en cada corrida.
   Lo consume Control de Gestión (mes x tienda x categoría).
   Importes en soles y sin IGV.
   ===================================================================== */
CREATE OR ALTER PROCEDURE rpt.usp_margen_mensual
AS
BEGIN
    SET NOCOUNT ON;

    CREATE TABLE #lineas (
        id_tienda      VARCHAR(4)     NOT NULL,
        mes            DATE           NOT NULL,
        sku_erp        VARCHAR(20)    NULL,
        categoria      VARCHAR(20)    NULL,
        ticket         VARCHAR(30)    NULL,
        unidades       INT            NOT NULL,
        venta_sin_igv  DECIMAL(12,2)  NULL
    );

    -- 1. Tiendas: boletas y facturas suman; notas de crédito restan.
    INSERT INTO #lineas (id_tienda, mes, sku_erp, categoria, ticket, unidades, venta_sin_igv)
    SELECT
        v.id_tienda,
        DATEFROMPARTS(YEAR(v.fecha_emision), MONTH(v.fecha_emision), 1),
        m.sku_erp,
        p.categoria,
        CASE WHEN v.tipo_comprobante <> '07'
             THEN CONCAT(v.tipo_comprobante, '-', v.serie, '-', v.numero) END,
        CASE WHEN v.tipo_comprobante = '07' THEN -v.cantidad ELSE v.cantidad END,
        CAST(CASE WHEN v.tipo_comprobante = '07' THEN -v.importe_total ELSE v.importe_total END
             / 1.18 AS DECIMAL(12,2))
    FROM stg.ventas_pos v
    LEFT JOIN stg.mapeo_sku m ON m.sku_pos = v.sku
    LEFT JOIN stg.productos p ON p.sku_erp = m.sku_erp
    WHERE v.tipo_comprobante IN ('01', '03', '07')
      AND v.estado <> 'ANULADO'
      AND p.categoria <> 'SERVICIO';   -- el delivery no lleva costo: queda fuera del reporte de margen

    -- 2. Tienda web: se reporta como una tienda más. Los pedidos en dólares se pasan a soles.
    INSERT INTO #lineas (id_tienda, mes, sku_erp, categoria, ticket, unidades, venta_sin_igv)
    SELECT
        'WEB',
        DATEFROMPARTS(YEAR(w.fecha_pedido), MONTH(w.fecha_pedido), 1),
        m.sku_erp,
        p.categoria,
        w.id_pedido,
        w.cantidad,
        CAST(CASE WHEN w.moneda = 'USD' THEN w.importe * tc.tc ELSE w.importe END
             / 1.18 AS DECIMAL(12,2))
    FROM stg.pedidos_web w
    LEFT JOIN stg.mapeo_sku m ON m.sku_web = w.sku_web
    LEFT JOIN stg.productos p ON p.sku_erp = m.sku_erp
    LEFT JOIN stg.tipo_cambio tc ON tc.fecha = CAST(w.fecha_pedido AS DATE)
    WHERE w.estado = 'PAGADO'
      AND p.categoria <> 'SERVICIO';

    -- 3. Costo vigente de cada producto.
    SELECT l.*, CAST(l.unidades * c.costo_unitario AS DECIMAL(14,4)) AS costo
    INTO #con_costo
    FROM #lineas l
    LEFT JOIN stg.costos_historico c
           ON c.sku_erp = l.sku_erp
          AND c.vigente_hasta IS NULL;

    -- 4. Reporte.
    TRUNCATE TABLE rpt.margen_mensual;

    WITH agg AS (
        SELECT mes, id_tienda, categoria,
               SUM(unidades)                     AS unidades,
               COUNT(DISTINCT ticket)            AS tickets,
               SUM(venta_sin_igv)                AS venta_neta,
               CAST(SUM(costo) AS DECIMAL(14,2)) AS costo
        FROM #con_costo
        GROUP BY mes, id_tienda, categoria
    )
    INSERT INTO rpt.margen_mensual
        (mes, id_tienda, categoria, unidades, tickets, unidades_por_ticket,
         venta_neta, costo, margen, margen_pct)
    SELECT mes, id_tienda, categoria, unidades, tickets,
           unidades / NULLIF(tickets, 0),
           venta_neta, costo, venta_neta - costo,
           CAST(100.0 * (venta_neta - costo) / NULLIF(venta_neta, 0) AS DECIMAL(9,2))
    FROM agg;

    DROP TABLE #lineas;
    DROP TABLE #con_costo;
END
GO
