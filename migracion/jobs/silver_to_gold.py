"""Job: silver -> gold.

Builds the monthly margin report (replica of rpt.usp_margen_mensual) from silver,
writes it as a Delta table in gold and, optionally, exports it as CSV with the output contract:
    mes,id_tienda,categoria,unidades,tickets,unidades_por_ticket,venta_neta,costo,margen,margen_pct
The report is fully recomputed on every run (like the SP's TRUNCATE + INSERT), so it is idempotent.

Usage:
    python -m migracion.jobs.silver_to_gold \
        --silver_path migracion/data/2_silver --gold_path migracion/data/3_gold \
        --output_file salida/rpt_margen_mensual_lote1.csv
"""
from pyspark.sql import DataFrame

from migracion.reports.legacy_margin import build_legacy_margin
from migracion.utils.csv_export import write_single_csv
from migracion.utils.spark import get_spark, read_args

REPORT_TABLE = "margen_mensual"
SORT_KEYS = ["mes", "id_tienda", "categoria"]


def write_gold(df: DataFrame, gold_path: str, table: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .save(f"{gold_path}/{table}")
    )


def main() -> None:
    args = read_args(["silver_path", "gold_path"], {"output_file": ""})
    spark = get_spark("silver_to_gold")

    report = build_legacy_margin(spark, args["silver_path"])
    write_gold(report, args["gold_path"], REPORT_TABLE)
    gold = spark.read.format("delta").load(f"{args['gold_path']}/{REPORT_TABLE}")
    print(f"[gold] {REPORT_TABLE} {gold.count()} rows")

    if args["output_file"]:
        write_single_csv(spark, gold.orderBy(*SORT_KEYS), args["output_file"])
        print(f"[gold] exported -> {args['output_file']}")

    spark.stop()


if __name__ == "__main__":
    main()
