"""Job landing -> bronze.

Lee los CSV de una fecha de carga y los escribe como tablas Delta en bronze.
- Todo se lee como texto: bronze guarda el dato crudo (sin perder ceros a la izquierda
  ni espacios). Los tipos del DDL se aplican en silver.
- Columnas de auditoría: _fecha_carga, _archivo, _fecha_ingesta.
- Overwrite por tabla: bronze contiene solo la carga en curso; el histórico queda en landing.
  Reprocesar la misma fecha deja el mismo resultado.

Uso:
    python -m migracion.jobs.landing_a_bronze \
        --ruta_landing migracion/data/0_landing --ruta_bronze migracion/data/1_bronze \
        --fecha_carga 2026-09-01
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from migracion.utils.spark import leer_argumentos, obtener_spark
from migracion.utils.tablas import TABLAS


def leer_landing(spark: SparkSession, ruta_landing: str, tabla: str, fecha_carga: str) -> DataFrame:
    ruta = f"{ruta_landing}/{tabla}/fecha_carga={fecha_carga}"
    return (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .option("inferSchema", False)  # todo como string
        .option("mode", "FAILFAST")    # una fila mal formada detiene el job
        .csv(ruta)
        .withColumn("_fecha_carga", F.lit(fecha_carga).cast("date"))
        .withColumn("_archivo", F.input_file_name())
        .withColumn("_fecha_ingesta", F.current_timestamp())
    )


def escribir_bronze(df: DataFrame, ruta_bronze: str, tabla: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .save(f"{ruta_bronze}/{tabla}")
    )


def main() -> None:
    args = leer_argumentos(["ruta_landing", "ruta_bronze", "fecha_carga"])
    spark = obtener_spark("landing_a_bronze")

    for tabla in TABLAS:
        df = leer_landing(spark, args["ruta_landing"], tabla, args["fecha_carga"])
        escribir_bronze(df, args["ruta_bronze"], tabla)
        filas = spark.read.format("delta").load(f"{args['ruta_bronze']}/{tabla}").count()
        print(f"[bronze] {tabla:<18} {filas:>8} filas (fecha_carga={args['fecha_carga']})")

    spark.stop()


if __name__ == "__main__":
    main()
