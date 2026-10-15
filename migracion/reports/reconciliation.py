"""Reconciliation: bridge from the legacy report (SQL Server logic) to Accounting, cause by cause.

Works line by line, so every sol is attributed to exactly one cause:
- Sales: legacy total + sales the SP dropped (by first reason) + USD sales the SP left as NULL
  = corrected total = Accounting.
- Cost: legacy total + cost of dropped lines (at sale date) + (cost at sale date - today's cost)
  on the lines the SP kept = corrected total = Accounting.
The legacy report rounds cost per row; that rounding is shown as its own line.

Usage (prints Markdown tables used in CONCILIACION.md):
    python -m migracion.reports.reconciliation \
        --silver_path migracion/data/2_silver \
        --accounting_file datos/referencia/registro_ventas_contable.csv
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from migracion.reports.corrected_margin import build_corrected_lines
from migracion.reports.legacy_margin import build_legacy_margin
from migracion.utils.spark import get_spark, read_args

# Order matters: a line dropped for several reasons is attributed to the first one.
CAUSES = [
    ("estado_null", "Ventas con estado vacio descartadas por el SP"),
    ("sin_mapeo", "Productos sin mapeo descartados por el SP"),
    ("delivery", "Delivery excluido por el SP"),
    ("usd_sin_tc", "Pedidos USD de dias sin tipo de cambio (venta NULL en el SP)"),
    ("descontinuado", "Productos descontinuados: sin costo vigente hoy, el SP no les pone costo"),
    ("costo_fecha", "Costo vigente hoy en vez de costo a la fecha de venta"),
    ("redondeo", "Redondeo del costo por fila en el reporte"),
]

LINES_SQL = """
SELECT l.*,
       CASE WHEN l.id_tienda = 'WEB' THEN 'WEB' ELSE 'TIENDAS' END AS canal,
       CASE WHEN l.estado IS NULL                  THEN 'estado_null'
            WHEN NOT l.mapeado                     THEN 'sin_mapeo'
            WHEN upper(l.categoria) = 'SERVICIO'   THEN 'delivery'
       END AS descartada_por,
       CAST(l.unidades * hoy.costo_unitario AS DECIMAL(14,4)) AS costo_hoy
FROM corrected_lines l
LEFT JOIN costos_historico hoy
       ON upper(rtrim(hoy.sku_erp)) = upper(rtrim(l.sku_erp)) AND hoy.vigente_hasta IS NULL
"""


def build_bridge(spark: SparkSession, silver_path: str) -> DataFrame:
    """One row per (mes, canal, concepto) with its effect on venta and costo, in soles."""
    legacy = (
        build_legacy_margin(spark, silver_path)
        .withColumn("canal", F.when(F.col("id_tienda") == "WEB", "WEB").otherwise("TIENDAS"))
        .groupBy("mes", "canal")
        .agg(F.sum("venta_neta").alias("venta"), F.sum("costo").alias("costo"))
    )
    build_corrected_lines(spark, silver_path)
    lines = spark.sql(LINES_SQL)

    def total(column, condition):
        return F.sum(F.when(condition, F.col(column))).cast("decimal(18,4)")

    kept = F.col("descartada_por").isNull()
    has_cost_today = F.col("costo_hoy").isNotNull()
    effects = {
        # cause: (effect on venta, effect on costo)
        **{c: (total("venta_sin_igv", F.col("descartada_por") == c), total("costo", F.col("descartada_por") == c))
           for c in ("estado_null", "sin_mapeo", "delivery")},
        "usd_sin_tc": (total("venta_sin_igv", kept & (F.col("moneda") == "USD") & ~F.col("tc_publicado")), None),
        "descontinuado": (None, total("costo", kept & ~has_cost_today)),
        "costo_fecha": (None, F.sum(F.when(kept & has_cost_today, F.col("costo") - F.col("costo_hoy")))),
    }
    # Every effect is rounded to cents (like Accounting) so the bridge adds up exactly.
    aggregates = [
        (F.lit(0) if expr is None else F.coalesce(expr, F.lit(0))).cast("decimal(14,2)").alias(f"{kind}_{cause}")
        for cause, (venta, costo) in effects.items()
        for kind, expr in (("v", venta), ("c", costo))
    ]
    per_month = lines.groupBy("mes", "canal").agg(
        *aggregates,
        F.sum("venta_sin_igv").cast("decimal(14,2)").alias("venta_corregida"),
        F.sum("costo").cast("decimal(14,2)").alias("costo_corregido"),
    )
    joined = legacy.join(per_month, ["mes", "canal"])

    def row(concept, venta, costo):
        return F.struct(F.lit(concept).alias("concepto"),
                        venta.cast("decimal(14,2)").alias("venta"), costo.cast("decimal(14,2)").alias("costo"))

    rows = [row("reporte_sp", F.col("venta"), F.col("costo"))]
    rows += [row(c, F.col(f"v_{c}"), F.col(f"c_{c}")) for c in effects]
    # Residual: the legacy report rounds cost per row (mes x tienda x categoria); Accounting rounds per month.
    explained_cost = F.col("costo")
    for c in effects:
        explained_cost = explained_cost + F.col(f"c_{c}")
    rows.append(row("redondeo", F.lit(0), F.col("costo_corregido") - explained_cost))
    rows.append(row("corregido", F.col("venta_corregida"), F.col("costo_corregido")))

    return joined.select("mes", "canal", F.explode(F.array(*rows)).alias("r")).select("mes", "canal", "r.*")


def to_markdown(df: DataFrame, columns: list[str]) -> str:
    rows = df.select(*columns).collect()
    out = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    for r in rows:
        out.append("| " + " | ".join("" if r[c] is None else f"{r[c]:,.2f}" if not isinstance(r[c], str) else r[c]
                                     for c in columns) + " |")
    return "\n".join(out)


def main() -> None:
    args = read_args(["silver_path", "accounting_file"])
    spark = get_spark("reconciliation")
    spark.sparkContext.setLogLevel("ERROR")

    accounting = spark.read.option("header", True).csv(args["accounting_file"]).selectExpr(
        "CAST(mes AS DATE) mes", "canal",
        "CAST(venta_neta AS DECIMAL(14,2)) venta", "CAST(costo_ventas AS DECIMAL(14,2)) costo")
    months = [r[0] for r in accounting.select("mes").distinct().collect()]
    bridge = build_bridge(spark, args["silver_path"]).cache()

    order = {name: i for i, name in enumerate(["reporte_sp"] + [c for c, _ in CAUSES] + ["corregido"])}
    order_col = F.create_map(*[x for k, v in order.items() for x in (F.lit(k), F.lit(v))])

    in_scope = bridge.where(F.col("mes").isin(months))
    total = in_scope.groupBy("canal", "concepto").agg(F.sum("venta").alias("venta"), F.sum("costo").alias("costo"))
    for canal in ("TIENDAS", "WEB"):
        print(f"\n### {canal} (ene-2025 a ago-2026)\n")
        print(to_markdown(total.where(F.col("canal") == canal).orderBy(order_col[F.col("concepto")]),
                          ["concepto", "venta", "costo"]))

    print("\n### Corregido vs Contabilidad por mes y canal\n")
    corrected = in_scope.where("concepto = 'corregido'").select(
        "mes", "canal", F.col("venta").alias("venta_corr"), F.col("costo").cast("decimal(14,2)").alias("costo_corr"))
    check = accounting.join(corrected, ["mes", "canal"], "left").select(
        "mes", "canal", (F.col("venta") - F.col("venta_corr")).alias("dif_venta"),
        (F.col("costo") - F.col("costo_corr")).alias("dif_costo"))
    print(f"meses-canal con diferencia 0.00 en venta y costo: "
          f"{check.where('dif_venta = 0 AND dif_costo = 0').count()} de {check.count()}")

    print("\n### Septiembre 2026 (sin registro contable)\n")
    sept = bridge.where(~F.col("mes").isin(months)).groupBy("concepto").agg(
        F.sum("venta").alias("venta"), F.sum("costo").alias("costo")).orderBy(order_col[F.col("concepto")])
    print(to_markdown(sept, ["concepto", "venta", "costo"]))

    spark.stop()


if __name__ == "__main__":
    main()
