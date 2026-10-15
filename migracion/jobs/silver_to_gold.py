"""Job: silver -> gold.

Builds two monthly margin reports from silver and writes them as Delta tables in gold:
- margen_mensual: faithful replica of rpt.usp_margen_mensual (same numbers as SQL Server).
- margen_mensual_corregido: with the corrections that reconcile with Accounting.
Optionally exports each one as CSV with the output contract:
    mes,id_tienda,categoria,unidades,tickets,unidades_por_ticket,venta_neta,costo,margen,margen_pct
Reports are fully recomputed on every run (like the SP's TRUNCATE + INSERT), so it is idempotent.

Usage:
    python -m migracion.jobs.silver_to_gold \
        --silver_path migracion/data/2_silver --gold_path migracion/data/3_gold \
        --output_file salida/rpt_margen_mensual_lote2.csv \
        --corrected_output_file salida/rpt_margen_mensual_lote2_corregido.csv
"""
from pyspark.sql import DataFrame, SparkSession

from migracion.reports.corrected_margin import build_corrected_margin
from migracion.reports.legacy_margin import build_legacy_margin
from migracion.utils.csv_export import write_single_csv
from migracion.utils.spark import get_spark, read_args

SORT_KEYS = ["mes", "id_tienda", "categoria"]
REPORTS = {
    "margen_mensual": (build_legacy_margin, "output_file"),
    "margen_mensual_corregido": (build_corrected_margin, "corrected_output_file"),
}


def write_gold(df: DataFrame, gold_path: str, table: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .save(f"{gold_path}/{table}")
    )


def publish_report(spark: SparkSession, df: DataFrame, gold_path: str, table: str, csv_path: str) -> None:
    write_gold(df, gold_path, table)
    gold = spark.read.format("delta").load(f"{gold_path}/{table}")
    print(f"[gold] {table:<26} {gold.count():>6} rows")
    if csv_path:
        write_single_csv(spark, gold.orderBy(*SORT_KEYS), csv_path)
        print(f"[gold] exported -> {csv_path}")


def main() -> None:
    args = read_args(["silver_path", "gold_path"], {"output_file": "", "corrected_output_file": ""})
    spark = get_spark("silver_to_gold")

    for table, (build, csv_arg) in REPORTS.items():
        publish_report(spark, build(spark, args["silver_path"]), args["gold_path"], table, args[csv_arg])

    spark.stop()


if __name__ == "__main__":
    main()
