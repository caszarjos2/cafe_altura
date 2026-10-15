"""Write a DataFrame as one CSV file with an exact name (local path or s3://)."""
from pyspark.sql import DataFrame, SparkSession


def write_single_csv(spark: SparkSession, df: DataFrame, path: str) -> None:
    """Write `df` to exactly `path` (header, comma, NULL as empty field, LF line endings).

    Spark always writes a folder of part files, so we write to a temporary folder and move the
    single part file to `path` with the Hadoop FileSystem API (works for local and S3).
    """
    tmp_dir = f"{path}._tmp"
    (
        df.coalesce(1).write.mode("overwrite")
        .option("header", True)
        .option("nullValue", "")
        .option("emptyValue", "")
        .option("lineSep", "\n")
        .csv(tmp_dir)
    )

    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    tmp = jvm.org.apache.hadoop.fs.Path(tmp_dir)
    fs = tmp.getFileSystem(conf)
    (part,) = [s.getPath() for s in fs.listStatus(tmp) if s.getPath().getName().startswith("part-")]
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs.mkdirs(target.getParent())
    fs.delete(target, False)
    if not fs.rename(part, target):
        raise IOError(f"Could not move {part} to {path}")
    fs.delete(tmp, True)
    # The local filesystem leaves a hidden .crc checksum next to the file: not part of the output.
    fs.delete(jvm.org.apache.hadoop.fs.Path(target.getParent(), f".{target.getName()}.crc"), False)
