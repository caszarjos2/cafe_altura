"""Job: bronze -> silver (SCD Type 1 upsert by primary key).

Equivalent to the stg tables the SSIS package loads into SQL Server:
1. Cast bronze strings to the DDL types (fails if a non-empty value cannot be cast).
2. Deduplicate the load by primary key: the latest fecha_actualizacion wins.
3. MERGE into silver by primary key:
   - matched and the incoming row is newer or equal and actually different -> update
   - not matched -> insert
   No deletes and no history (SCD1): history is kept in landing/bronze.
Audit column: _load_date (load that last inserted or changed the row).
Fact tables are partitioned by month (partition_month).

Running the same load twice leaves silver unchanged.

Usage:
    python -m migracion.jobs.bronze_to_silver \
        --bronze_path migracion/data/1_bronze --silver_path migracion/data/2_silver
"""
from pyspark.sql import DataFrame, SparkSession

from migracion.utils.delta_io import last_operation_metrics, upsert
from migracion.utils.spark import get_spark, read_args
from migracion.utils.tables import (
    PARTITION_COLUMN,
    PARTITION_SOURCE,
    PRIMARY_KEYS,
    SCHEMAS,
    TABLES,
    VERSION_COLUMN,
)
from migracion.utils.transforms import add_month_partition, cast_to_schema, deduplicate

AUDIT_COLUMN = "_load_date"


def prepare(bronze: DataFrame, table: str) -> DataFrame:
    """Typed, deduplicated and partitioned rows of one table, ready to merge."""
    schema = SCHEMAS[table]
    version = VERSION_COLUMN if VERSION_COLUMN in schema else None
    df = cast_to_schema(bronze, schema, keep=[AUDIT_COLUMN])
    df = deduplicate(df, PRIMARY_KEYS[table], order_by=version)
    if table in PARTITION_SOURCE:
        df = add_month_partition(df, PARTITION_SOURCE[table], PARTITION_COLUMN)
    return df


def process_table(spark: SparkSession, bronze_path: str, silver_path: str, table: str) -> None:
    bronze = spark.read.format("delta").load(f"{bronze_path}/{table}")
    schema = SCHEMAS[table]
    upsert(
        spark,
        prepare(bronze, table),
        target_path=f"{silver_path}/{table}",
        keys=PRIMARY_KEYS[table],
        compare_columns=list(schema),
        version_column=VERSION_COLUMN if VERSION_COLUMN in schema else None,
        partition_by=[PARTITION_COLUMN] if table in PARTITION_SOURCE else None,
    )


def main() -> None:
    args = read_args(["bronze_path", "silver_path"])
    spark = get_spark("bronze_to_silver")

    for table in TABLES:
        process_table(spark, args["bronze_path"], args["silver_path"], table)
        path = f"{args['silver_path']}/{table}"
        rows = spark.read.format("delta").load(path).count()
        metrics = last_operation_metrics(spark, path)
        inserted = metrics.get("numTargetRowsInserted", metrics.get("numOutputRows"))
        print(f"[silver] {table:<18} {rows:>8} rows | inserted={inserted} updated={metrics.get('numTargetRowsUpdated', 0)}")

    spark.stop()


if __name__ == "__main__":
    main()
