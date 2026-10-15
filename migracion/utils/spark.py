"""SparkSession con Delta y lectura de argumentos, igual en local que en AWS Glue.

- En Glue (5.1) el job se crea con `--datalake-formats delta`, que agrega los jars de Delta;
  aquí solo se configuran las extensiones.
- En local se usa `configure_spark_with_delta_pip` (pyspark==3.5.6, delta-spark==3.3.2).
- Los argumentos llegan como `--nombre valor` en ambos casos, así que las rutas pueden ser
  locales o `s3://`.
"""
import argparse
import os
import sys

from pyspark.sql import SparkSession

DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # Las fechas del origen no traen zona horaria: se interpretan tal cual, sin desplazamientos.
    "spark.sql.session.timeZone": "UTC",
}


def en_glue() -> bool:
    try:
        import awsglue  # noqa: F401
        return True
    except ImportError:
        return False


def obtener_spark(app: str = "cafe-altura") -> SparkSession:
    builder = SparkSession.builder.appName(app)
    for clave, valor in DELTA_CONF.items():
        builder = builder.config(clave, valor)

    if en_glue():
        return builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # los workers usan este mismo Python
    builder = (
        builder.master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")  # datos chicos: evita 200 tareas vacías
        .config("spark.ui.showConsoleProgress", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def leer_argumentos(obligatorios: list[str], opcionales: dict[str, str] | None = None) -> dict[str, str]:
    """Lee `--nombre valor`. En Glue usa getResolvedOptions; en local, argparse."""
    opcionales = opcionales or {}
    if en_glue():
        from awsglue.utils import getResolvedOptions

        presentes = [o for o in opcionales if f"--{o}" in sys.argv]
        args = getResolvedOptions(sys.argv, obligatorios + presentes)
        return {**opcionales, **args}

    parser = argparse.ArgumentParser()
    for nombre in obligatorios:
        parser.add_argument(f"--{nombre}", required=True)
    for nombre, defecto in opcionales.items():
        parser.add_argument(f"--{nombre}", default=defecto)
    return vars(parser.parse_known_args()[0])
