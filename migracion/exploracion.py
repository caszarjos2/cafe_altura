import duckdb

RAIZ = "C:/Users/Zarat/Downloads/reto_cafe_altura/reto_cafe_altura"
RPT = f"{RAIZ}/legado/salida/rpt_margen_mensual_lote1.csv"
RPT2 = f"{RAIZ}/legado/salida/rpt_margen_mensual_lote2.csv"
CONT = f"{RAIZ}/datos/referencia/registro_ventas_contable.csv"

con = duckdb.connect()

# Rango y cantidad de meses en cada fuente
con.sql(f"""
    SELECT 'rpt_margen_mensual_lote1' AS fuente, MIN(mes) AS mes_min, MAX(mes) AS mes_max,
           COUNT(DISTINCT mes) AS meses
    FROM read_csv('{RPT}')
    UNION ALL
    SELECT 'rpt_margen_mensual_lote1' AS fuente, MIN(mes) AS mes_min, MAX(mes) AS mes_max,
           COUNT(DISTINCT mes) AS meses
    FROM read_csv('{RPT2}')
    UNION ALL
    SELECT 'registro_ventas_contable', MIN(mes), MAX(mes), COUNT(DISTINCT mes)
    FROM read_csv('{CONT}')
""").show()

# rpt lote 1 agrupado igual que el registro contable: mes + canal (WEB / TIENDAS)
con.sql(f"""
    CREATE TABLE rpt_canal AS
    SELECT mes,
           CASE WHEN id_tienda = 'WEB' THEN 'WEB' ELSE 'TIENDAS' END AS canal,
           SUM(CAST(venta_neta AS DECIMAL(14,2))) AS venta_neta,
           SUM(CAST(costo AS DECIMAL(14,2)))      AS costo_ventas
    FROM read_csv('{RPT}')
    GROUP BY ALL
""")
con.sql("FROM rpt_canal ORDER BY mes, canal").show(max_rows=50)

# Registro contable
con.sql(f"""
    CREATE TABLE contable AS
    SELECT CAST(mes AS DATE)                   AS mes,
           canal,
           CAST(venta_neta AS DECIMAL(14,2))   AS venta_neta,
           CAST(costo_ventas AS DECIMAL(14,2)) AS costo_ventas
    FROM read_csv('{CONT}')
""")

# Comparación por mes y canal (diferencia = contable - rpt)
con.sql("""
    CREATE TABLE comparacion AS
    SELECT c.mes, c.canal,
           c.venta_neta                    AS venta_contable,
           r.venta_neta                    AS venta_rpt,
           c.venta_neta - r.venta_neta     AS dif_venta,
           c.costo_ventas                  AS costo_contable,
           r.costo_ventas                  AS costo_rpt,
           c.costo_ventas - r.costo_ventas AS dif_costo
    FROM contable c
    FULL JOIN rpt_canal r USING (mes, canal)
""")
con.sql("FROM comparacion ORDER BY canal, mes").show(max_rows=50)

# Totales por canal
con.sql("""
    SELECT canal,
           SUM(venta_contable) AS venta_contable,
           SUM(venta_rpt)      AS venta_rpt,
           SUM(dif_venta)      AS dif_venta,
           ROUND(100 * SUM(dif_venta) / SUM(venta_contable), 2) AS pct_dif_venta,
           SUM(costo_contable) AS costo_contable,
           SUM(costo_rpt)      AS costo_rpt,
           SUM(dif_costo)      AS dif_costo,
           ROUND(100 * SUM(dif_costo) / SUM(costo_contable), 2) AS pct_dif_costo
    FROM comparacion
    GROUP BY ROLLUP (canal)
    ORDER BY canal NULLS LAST
""").show()
