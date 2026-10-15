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
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from migracion.utils.spark import get_spark, read_args
from migracion.utils.tables import (
    PARTITION_COLUMN,
    PARTITION_SOURCE,
    PRIMARY_KEYS,
    SCHEMAS,
    TABLES,
    VERSION_COLUMN,
)

AUDIT_COLUMN = "_load_date"


def cast_to_schema(df: DataFrame, table: str) -> DataFrame:
    """Cast raw strings to DDL types. Fails if any non-empty value turns into NULL."""
    schema = SCHEMAS[table]
    typed = df.select(
        *[F.col(c).cast(t).alias(c) for c, t in schema.items()],
        *[F.col(c).alias(f"_raw_{c}") for c in schema],
        F.col(AUDIT_COLUMN),
    )
    bad = typed.where(
        " OR ".join(f"(_raw_{c} IS NOT NULL AND {c} IS NULL)" for c, t in schema.items() if t != "string")
        or "false"
    )
    bad_rows = bad.count()
    if bad_rows:
        bad.show(5, truncate=False)
        raise ValueError(f"{table}: {bad_rows} rows with values that do not match the DDL types")
    return typed.select(*schema, AUDIT_COLUMN)


def deduplicate(df: DataFrame, table: str) -> DataFrame:
    """Keep one row per primary key: latest fecha_actualizacion; exact ties broken deterministically."""
    order = [F.col(VERSION_COLUMN).desc()] if VERSION_COLUMN in df.columns else []
    order.append(F.xxhash64(*SCHEMAS[table]).desc())
    window = Window.partitionBy(*PRIMARY_KEYS[table]).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def add_partition(df: DataFrame, table: str) -> DataFrame:
    if table not in PARTITION_SOURCE:
        return df
    return df.withColumn(PARTITION_COLUMN, F.trunc(F.col(PARTITION_SOURCE[table]), "month"))


def upsert(spark: SparkSession, source: DataFrame, silver_path: str, table: str) -> None:
    """SCD1 MERGE by primary key. Creates the table on the first load."""
    target_path = f"{silver_path}/{table}"
    if not DeltaTable.isDeltaTable(spark, target_path):
        writer = source.write.format("delta").mode("overwrite")
        if table in PARTITION_SOURCE:
            writer = writer.partitionBy(PARTITION_COLUMN)
        writer.save(target_path)
        return

    data_columns = list(SCHEMAS[table])
    on = " AND ".join(f"t.{k} = s.{k}" for k in PRIMARY_KEYS[table])
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in data_columns)
    newer = f"s.{VERSION_COLUMN} >= t.{VERSION_COLUMN}" if VERSION_COLUMN in data_columns else "true"

    (
        DeltaTable.forPath(spark, target_path).alias("t")
        .merge(source.alias("s"), on)
        .whenMatchedUpdateAll(condition=f"({newer}) AND ({changed})")
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_table(spark: SparkSession, bronze_path: str, silver_path: str, table: str) -> None:
    bronze = spark.read.format("delta").load(f"{bronze_path}/{table}")
    source = add_partition(deduplicate(cast_to_schema(bronze, table), table), table)
    upsert(spark, source, silver_path, table)


def main() -> None:
    args = read_args(["bronze_path", "silver_path"])
    spark = get_spark("bronze_to_silver")

    for table in TABLES:
        process_table(spark, args["bronze_path"], args["silver_path"], table)
        path = f"{args['silver_path']}/{table}"
        rows = spark.read.format("delta").load(path).count()
        metrics = DeltaTable.forPath(spark, path).history(1).select("operationMetrics").first()[0]
        print(
            f"[silver] {table:<18} {rows:>8} rows | inserted={metrics.get('numTargetRowsInserted', metrics.get('numOutputRows'))}"
            f" updated={metrics.get('numTargetRowsUpdated', 0)}"
        )

    spark.stop()


if __name__ == "__main__":
    main()
