# Café Altura: migración del reporte de margen mensual a PySpark + Delta

Migración del stored procedure `rpt.usp_margen_mensual` (SQL Server) a un pipeline PySpark sobre Delta Lake, listo para AWS Glue. El enunciado original está en [ENUNCIADO.md](ENUNCIADO.md).

| Resultado | Dónde |
|---|---|
| El reporte del pipeline es **idéntico byte a byte** al de SQL Server (lote 1 y lote 2) | `salida/rpt_margen_mensual_lote{1,2}.csv`, `tests/test_pipeline_parity.py` |
| Carga incremental con **MERGE** (upsert SCD1) e idempotente | `migracion/jobs/bronze_to_silver.py`, `tests/test_bronze_to_silver.py` |
| Explicación de por qué no cuadra con Contabilidad: 6 causas, en soles | [CONCILIACION.md](CONCILIACION.md) |
| Reporte corregido que **cuadra 40/40 al céntimo** con Contabilidad | `salida/rpt_margen_mensual_lote2_corregido.csv`, `tests/test_corrected_margin.py` |
| Nota para Control de Gestión | [NOTA_USUARIO.md](NOTA_USUARIO.md) |
| Despliegue en Glue | [DESPLIEGUE_GLUE.md](DESPLIEGUE_GLUE.md) |
| Uso de IA | [AI_LOG.md](AI_LOG.md) |

## Arquitectura

```
datos/lote_N/*.csv
   │  scripts/upload_landing.sh
   ▼
0_landing/load_date=AAAA-MM-DD/<tabla>.csv     CSV tal cual; histórico de todas las cargas
   │  jobs/landing_to_bronze.py
   ▼
1_bronze/<tabla>   Delta, todo como texto + _load_date, _source_file, _ingested_at (overwrite por carga)
   │  jobs/bronze_to_silver.py
   ▼
2_silver/<tabla>   Delta tipado según el DDL; dedup por PK; MERGE SCD1 (= tablas stg de SQL Server)
   │  jobs/silver_to_gold.py
   ▼
3_gold/margen_mensual             réplica exacta del SP          → salida/rpt_margen_mensual_loteN.csv
3_gold/margen_mensual_corregido   con las correcciones           → salida/rpt_margen_mensual_lote2_corregido.csv
```

```
migracion/
├── jobs/        landing_to_bronze.py, bronze_to_silver.py, silver_to_gold.py   (un job de Glue cada uno)
├── reports/     legacy_margin.py (réplica del SP), corrected_margin.py, reconciliation.py
├── utils/       spark.py (sesión + argumentos local/Glue), tables.py (PK y tipos del DDL),
│                transforms.py, delta_io.py (upsert), csv_export.py
├── scripts/     upload_landing.sh, run_batch.sh
└── data/        data lake local (ignorado por git)
tests/           silver (unitarios), paridad con SQL Server y cuadre contable (end-to-end)
```

## Entorno

Se usa la referencia del enunciado, **AWS Glue 5.1**: Spark 3.5.6, Python 3.11, Delta 3.3.2 y Java 17. Se desarrolló en **WSL2 (Ubuntu 24.04)**, sin `winutils`.

```bash
# Python 3.11 con uv y Java 17 (Temurin), sin sudo
curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv --python 3.11 ~/.venvs/cafe-altura
uv pip install --python ~/.venvs/cafe-altura/bin/python -r requirements.txt
export JAVA_HOME=~/.jdks/jdk-17          # o cualquier JDK 17
source ~/.venvs/cafe-altura/bin/activate
```

## Cómo ejecutar, lote por lote

Desde la raíz del repo. Cada lote recorre landing → bronze → silver → gold y exporta el CSV:

```bash
# Lote 1 (carga inicial, datos hasta el 31-ago-2026)
bash migracion/scripts/run_batch.sh datos/lote_1 2026-09-01 salida/rpt_margen_mensual_lote1.csv

# Lote 2 (incremental: 1 al 14-sep-2026 + correcciones de agosto), también exporta el corregido
bash migracion/scripts/run_batch.sh datos/lote_2 2026-09-15 salida/rpt_margen_mensual_lote2.csv \
     salida/rpt_margen_mensual_lote2_corregido.csv
```

Los jobs también se pueden correr uno a uno, como se haría en Glue:

```bash
python -m migracion.jobs.landing_to_bronze --landing_path migracion/data/0_landing \
       --bronze_path migracion/data/1_bronze --load_date 2026-09-15
python -m migracion.jobs.bronze_to_silver --bronze_path migracion/data/1_bronze --silver_path migracion/data/2_silver
python -m migracion.jobs.silver_to_gold --silver_path migracion/data/2_silver --gold_path migracion/data/3_gold \
       --output_file salida/rpt_margen_mensual_lote2.csv \
       --corrected_output_file salida/rpt_margen_mensual_lote2_corregido.csv
```

El puente de conciliación (tablas de `CONCILIACION.md`) se obtiene con:

```bash
python -m migracion.reports.reconciliation --silver_path migracion/data/2_silver \
       --accounting_file datos/referencia/registro_ventas_contable.csv
```

Los tests tardan unos 3 a 4 minutos, porque corren los lotes reales de punta a punta:

```bash
python -m pytest tests
```

Para empezar de cero, borra el contenido de `migracion/data/*/` (menos los `.gitkeep`).

## Decisiones y supuestos

- **Fechas de carga:** el lote 1 tiene actualizaciones hasta el 31-ago-2026 y el lote 2 hasta el 14-sep-2026. Como el SSIS corre de noche, se asignó `load_date` 2026-09-01 y 2026-09-15.
- **Landing ordenado por fecha y luego tabla**, para ver de un vistazo todo lo que llegó en un día.
- **Bronze en overwrite por carga.** El histórico ya está en landing, así que bronze no necesita acumular.
- **Silver con upsert SCD Tipo 1 por PK en las 7 tablas**, igual que el SSIS:
  - Dentro de la carga gana la `fecha_actualizacion` más reciente (el lote 2 trae 240 llaves POS y 49 web repetidas).
  - Una versión más vieja nunca pisa a una más nueva.
  - No se borran filas.
  - `_load_date` indica la carga que insertó o cambió la fila por última vez.
- **Partición mensual** solo en `ventas_pos` y `pedidos_web`. Los maestros son chicos y no se particionan.
- **La réplica del SP está en Spark SQL**, espejo línea a línea de `usp_margen_mensual.sql`, para que se pueda comparar a simple vista. El enunciado permite DataFrame API o Spark SQL, y el plan de ejecución es el mismo.
- **Collation de SQL Server:** `Modern_Spanish_CI_AS` no distingue mayúsculas e ignora los espacios finales. Se replica con `upper(rtrim())` en los joins de SKU y en el filtro de categoría. Sin eso se pierden las tiendas de Cusco y aparece el delivery.
- **El reporte corregido va aparte** y no reemplaza al actual, porque el enunciado no acepta cambiar números sin avisar. Tiene 5 correcciones, marcadas con `-- CORRECCION n`; la sexta (las anulaciones tardías) ya la resuelve el MERGE.
- **Mapeo faltante por nombre de producto:** se usa la descripción de la boleta y el slug web. Se verificó que la descripción coincide con el nombre en el 100% de las líneas mapeadas; aun así, lo correcto es completar `mapeo_sku` en el ERP.
- **Delivery incluido en el corregido**, porque es venta para Contabilidad. Queda pendiente la decisión de Control de Gestión.
- **Las fechas no tienen zona horaria.** Spark se configura en UTC para interpretarlas tal cual, sin desplazamientos.
- **El código está en inglés y la documentación en español.** Las columnas de origen conservan los nombres del DDL.

## Fuera de alcance

- **Despliegue real en AWS:** el enunciado no requiere cuenta. El código ya está preparado: rutas `s3://`, `getResolvedOptions` y un solo CSV con nombre exacto vía la API de Hadoop. La infraestructura (jobs, Step Functions, IAM, catálogo) está descrita en `DESPLIEGUE_GLUE.md`, pero no está escrita como código (Terraform o CDK).
- **Hash de los datos personales en silver:** `cliente_email` y `cliente_doc` no se usan en el reporte. El tratamiento está propuesto en `DESPLIEGUE_GLUE.md`, pero no está implementado, para no alterar las tablas que replican el `stg` de SQL Server.
- **Glue Data Quality y alertas:** están descritas, no implementadas. Sí está implementado que el job falle si un valor no castea al tipo del DDL.
- **Corregido del lote 1:** el enunciado lo pide solo para el lote 2. Con solo el lote 1, agosto no cuadra, porque aún no llegan las anulaciones tardías.
- **`unidades_por_ticket` con decimales:** sigue siendo una división entera como en el SP, para respetar el tipo `INT` del contrato. Está anotado en `CONCILIACION.md`.
- **Docker de SQL Server:** no fue necesario. La paridad se valida contra las salidas entregadas en `legado/salida`.
- **`migracion/exploracion.py`:** es la exploración inicial con DuckDB (permitida para explorar). No es parte del pipeline.
