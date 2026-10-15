# Despliegue en AWS Glue

**Versión y tipo de job.** Glue 5.1 (Spark 3.5.6, Python 3.11, Delta 3.3.2, Java 17), con la misma versión con la que se desarrolló y probó en local. Tres jobs **Spark ETL** (`glueetl`), uno por capa: `landing_to_bronze`, `bronze_to_silver` y `silver_to_gold`. Los scripts son `migracion/jobs/*.py`; el paquete `migracion/` se sube como zip con `--extra-py-files`. Las rutas llegan por parámetro (`--landing_path`, `--load_date`, etc.) y `utils/spark.py` usa `getResolvedOptions` en Glue y `argparse` en local, así que el código es el mismo en los dos entornos. Cada job tiene `max concurrent runs = 1`, para que no corran dos MERGE a la vez.

**Workers.** 2 × G.1X (4 vCPU, 16 GB): el volumen es chico (~260 mil líneas POS en 20 meses, ~7 mil nuevas cada 2 semanas). En local (WSL), un lote completo, con los tres jobs y el arranque de Spark, tarda unos 3 minutos. Auto Scaling activado con un máximo de 4 workers, para absorber un backfill. Timeout de 30 minutos por job.

**Delta.** `--datalake-formats delta` más `--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`. Mantenimiento semanal: `OPTIMIZE` en silver (`ventas_pos` y `pedidos_web`) y `VACUUM` con retención de 7 días. El historial de Delta permite auditar y volver atrás (*time travel*) cualquier carga.

**Estructura en S3 y catálogo.** Un bucket por ambiente (`s3://cafe-altura-lake-prd/`), con cifrado SSE-KMS y acceso público bloqueado:

| Prefijo | Formato | Catálogo (Glue Data Catalog) |
|---|---|---|
| `0_landing/load_date=AAAA-MM-DD/<tabla>.csv` | CSV tal cual llega | — |
| `1_bronze/<tabla>/` | Delta, todo texto, overwrite por carga | `cafe_altura_bronze` |
| `2_silver/<tabla>/` | Delta tipado, upsert SCD1; hechos por `partition_month` | `cafe_altura_silver` |
| `3_gold/margen_mensual/` y `margen_mensual_corregido/` | Delta, recalculado completo | `cafe_altura_gold` |
| `exports/rpt_margen_mensual_<fecha>.csv` | CSV del contrato de salida | — |

Las tablas Delta se registran en el catálogo, así que se pueden consultar desde Athena o QuickSight sin copiar datos.

**Orquestación e incremental.** El sistema origen deja los 7 CSV en `0_landing/load_date=…/` (en local lo hace `upload_landing.sh`) y al final un archivo `_SUCCESS`. Una regla de EventBridge detecta el `_SUCCESS` y dispara una máquina de Step Functions: bronze → silver → gold → conciliación, pasando `load_date`. Como respaldo, un horario nocturno a las 02:00 procesa la última fecha pendiente. La carga incremental funciona así:
- Bronze solo tiene la carga del día.
- Silver hace `MERGE` por llave primaria: gana la `fecha_actualizacion` más reciente, no borra y nunca deja entrar una versión más vieja.
- Gold se recalcula completo, porque son ~5 mil filas.

Reprocesar una fecha es idempotente; está probado con tests. Para un backfill se reprocesan las `load_date` en orden.

**Monitoreo y alertas.**
- Una regla de EventBridge sobre `Glue Job State Change` (FAILED o TIMEOUT) y los fallos de Step Functions envían a SNS (correo al equipo de datos).
- Métricas de los jobs y logs en CloudWatch, con *continuous logging* activado.
- Controles de calidad que detienen el pipeline:
  - el job falla si un valor no castea al tipo del DDL (implementado);
  - Glue Data Quality en silver: llave primaria única y no nula, y `importe_total` no nulo.
- Controles que **alertan sin detener**:
  - número de ventas sin mapeo de producto mayor que 0;
  - días con pedidos en USD sin tipo de cambio;
  - diferencia del corregido contra el registro contable mayor que S/ 1 cuando llega el cierre mensual (lo calcula `reconciliation.py`).

**Datos personales.** `pedidos_web` trae `cliente_email` y `cliente_doc` (DNI/RUC), que están protegidos por la Ley 29733. El reporte no los usa. Se proponen cuatro medidas:
- En silver se reemplazan por un hash SHA-256 con *salt* guardado en Secrets Manager. Así se puede seguir contando clientes únicos sin exponer el dato.
- Solo landing y bronze conservan el dato original, con acceso restringido por Lake Formation al rol del pipeline.
- Una regla de ciclo de vida de S3 borra landing a los 90 días.
- Cifrado KMS en todo el bucket y CloudTrail para auditar los accesos.

**Costo aproximado** (us-east-1, US$ 0.44 por DPU-hora, con facturación por segundo y mínimo de 1 minuto):
- **Glue:** cada corrida usa 3 jobs × 2 DPU × ~2 minutos ≈ 0.2 DPU-hora, o sea ≈ US$ 0.09 por corrida. Una corrida diaria son **≈ US$ 3 al mes**; con Flex (US$ 0.29) bajaría a ≈ US$ 2.
- **S3, Step Functions, EventBridge y CloudWatch:** menos de US$ 2 al mes con este volumen.
- **Total:** menos de **US$ 5 al mes**.
