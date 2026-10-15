"""Job: landing -> bronze.

Reads the CSV files of one load date and writes them as Delta tables in bronze.
- Everything is read as string: bronze keeps the raw value (leading zeros and trailing
  spaces included). DDL types are applied in silver.
- Audit columns: _load_date, _source_file, _ingested_at.
- Overwrite per table: bronze only holds the current load; history lives in landing.
  Reprocessing the same date yields the same result.

Usage:
    python -m migracion.jobs.landing_to_bronze \
        --landing_path migracion/data/0_landing --bronze_path migracion/data/1_bronze \
        --load_date 2026-09-01
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from migracion.utils.spark import get_spark, read_args
from migracion.utils.tables import TABLES


def read_landing(spark: SparkSession, landing_path: str, table: str, load_date: str) -> DataFrame:
    path = f"{landing_path}/load_date={load_date}/{table}.csv"
    return (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .option("inferSchema", False)  # everything as string
        .option("mode", "FAILFAST")    # a malformed row stops the job
        .csv(path)
        .withColumn("_load_date", F.lit(load_date).cast("date"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_bronze(df: DataFrame, bronze_path: str, table: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .save(f"{bronze_path}/{table}")
    )


def main() -> None:
    args = read_args(["landing_path", "bronze_path", "load_date"])
    spark = get_spark("landing_to_bronze")

    for table in TABLES:
        df = read_landing(spark, args["landing_path"], table, args["load_date"])
        write_bronze(df, args["bronze_path"], table)
        rows = spark.read.format("delta").load(f"{args['bronze_path']}/{table}").count()
        print(f"[bronze] {table:<18} {rows:>8} rows (load_date={args['load_date']})")

    spark.stop()


if __name__ == "__main__":
    main()
