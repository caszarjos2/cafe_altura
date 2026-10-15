"""SparkSession with Delta and argument parsing that work the same locally and on AWS Glue.

- On Glue (5.1) the job is created with `--datalake-formats delta`, which provides the Delta
  jars; here we only set the extensions.
- Locally we use `configure_spark_with_delta_pip` (pyspark==3.5.6, delta-spark==3.3.2).
- Arguments arrive as `--name value` in both cases, so paths can be local or `s3://`.
"""
import argparse
import os
import sys

from pyspark.sql import SparkSession

DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # Source timestamps carry no time zone: read them as-is, without shifting.
    "spark.sql.session.timeZone": "UTC",
}


def is_glue() -> bool:
    try:
        import awsglue  # noqa: F401
        return True
    except ImportError:
        return False


def get_spark(app_name: str = "cafe-altura") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in DELTA_CONF.items():
        builder = builder.config(key, value)

    if is_glue():
        return builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # workers use this same Python
    builder = (
        builder.master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")  # small data: avoid 200 empty tasks
        .config("spark.ui.showConsoleProgress", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_args(required: list[str], optional: dict[str, str] | None = None) -> dict[str, str]:
    """Parse `--name value` args. Uses getResolvedOptions on Glue, argparse locally."""
    optional = optional or {}
    if is_glue():
        from awsglue.utils import getResolvedOptions

        present = [name for name in optional if f"--{name}" in sys.argv]
        args = getResolvedOptions(sys.argv, required + present)
        return {**optional, **args}

    parser = argparse.ArgumentParser()
    for name in required:
        parser.add_argument(f"--{name}", required=True)
    for name, default in optional.items():
        parser.add_argument(f"--{name}", default=default)
    return vars(parser.parse_known_args()[0])
