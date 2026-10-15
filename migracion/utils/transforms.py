"""Reusable DataFrame transformations."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def cast_to_schema(df: DataFrame, schema: dict[str, str], keep: list[str] | None = None) -> DataFrame:
    """Cast string columns to the given types, keeping `keep` columns as they are.

    Fails if any non-empty value cannot be cast (Spark would silently turn it into NULL).
    """
    keep = keep or []
    typed = df.select(
        *[F.col(c).cast(t).alias(c) for c, t in schema.items()],
        *[F.col(c).alias(f"_raw_{c}") for c in schema],
        *keep,
    )
    checks = [f"(_raw_{c} IS NOT NULL AND {c} IS NULL)" for c, t in schema.items() if t != "string"]
    if checks:
        bad = typed.where(" OR ".join(checks))
        bad_rows = bad.count()
        if bad_rows:
            bad.show(5, truncate=False)
            raise ValueError(f"{bad_rows} rows with values that do not match the expected types")
    return typed.select(*schema, *keep)


def deduplicate(df: DataFrame, keys: list[str], order_by: str | None = None) -> DataFrame:
    """Keep one row per key: highest `order_by` first; exact ties broken deterministically."""
    data_columns = [c for c in df.columns if not c.startswith("_")]
    order = [F.col(order_by).desc()] if order_by else []
    order.append(F.xxhash64(*data_columns).desc())
    window = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def add_month_partition(df: DataFrame, date_column: str, partition_column: str) -> DataFrame:
    """Add the first day of the month of `date_column` as partition column."""
    return df.withColumn(partition_column, F.trunc(F.col(date_column), "month"))
