"""Reusable Delta Lake writes."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def upsert(
    spark: SparkSession,
    source: DataFrame,
    target_path: str,
    keys: list[str],
    compare_columns: list[str],
    version_column: str | None = None,
    partition_by: list[str] | None = None,
) -> None:
    """SCD Type 1 MERGE by `keys`. Creates the table on the first load.

    - matched, incoming row is newer or equal (`version_column`) and any of `compare_columns`
      changed -> update all columns
    - not matched -> insert
    No deletes. Re-running the same source leaves the table unchanged.
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        writer = source.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)
        return

    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in compare_columns)
    newer = f"s.{version_column} >= t.{version_column}" if version_column else "true"

    (
        DeltaTable.forPath(spark, target_path).alias("t")
        .merge(source.alias("s"), on)
        .whenMatchedUpdateAll(condition=f"({newer}) AND ({changed})")
        .whenNotMatchedInsertAll()
        .execute()
    )


def last_operation_metrics(spark: SparkSession, path: str) -> dict[str, str]:
    """Metrics (inserted, updated, ...) of the latest commit of a Delta table."""
    return DeltaTable.forPath(spark, path).history(1).select("operationMetrics").first()[0]
