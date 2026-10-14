# Reto técnico — Data Engineer AWS

Gracias por continuar en el proceso de selección de Tuxpas. Este reto reproduce el trabajo del puesto: migrar a PySpark sobre Delta Lake un proceso de datos que hoy corre en SQL Server, de modo que quede listo para AWS Glue y el negocio siga recibiendo los mismos números.

---

## 1. Contexto

Café Altura es una cadena peruana de cafeterías con tostaduría propia. Tiene 40 tiendas en Lima, Arequipa, Trujillo, Cusco, Piura y Chiclayo, y una tienda web que vende en soles y en dólares.

Todas las noches, un paquete SSIS carga en SQL Server los datos del POS de las tiendas, de la tienda web y del ERP. Luego, un job de SQL Agent ejecuta el stored procedure `rpt.usp_margen_mensual`, que genera el reporte de margen mensual por tienda y categoría que usa el área de Control de Gestión.

La empresa está trasladando su plataforma de datos a AWS y ya definió la arquitectura de destino: AWS Glue, PySpark y tablas Delta sobre S3. El reto consiste en migrar este reporte.

Al presentar el proyecto, la jefa de Control de Gestión comentó lo siguiente:

> "Necesito que el reporte nuevo me dé lo mismo que el de hoy, o que me expliquen por qué no. Y otra cosa: el reporte nunca ha cuadrado del todo con contabilidad, y nadie ha sabido decirme por qué."

## 2. Material del reto

```
├── README.md                          este documento
├── datos/
│   ├── lote_1/                        carga inicial (enero 2025 a agosto 2026), 7 tablas en CSV
│   ├── lote_2/                        siguiente lote (1 al 14 de septiembre de 2026)
│   └── referencia/
│       └── registro_ventas_contable.csv   venta neta y costo por mes y canal, según Contabilidad
└── legado/
    ├── ddl.sql                        esquema de SQL Server: tablas stg y el reporte
    ├── usp_margen_mensual.sql         el stored procedure que arma el reporte
    ├── salida/
    │   ├── rpt_margen_mensual_lote1.csv   lo que produjo el SP con el lote 1 cargado
    │   └── rpt_margen_mensual_lote2.csv   lo que produjo después de cargar el lote 2
    └── docker/                        opcional: SQL Server con el lote 1 y el SP, para correrlo
```

Sobre los datos:

- Cada CSV corresponde a una tabla `stg` exportada sin cambios. Los tipos y las llaves primarias están en el [DDL](legado/ddl.sql).
- En el lote 2, `ventas_pos` y `pedidos_web` traen únicamente lo nuevo desde el lote anterior; las otras cinco tablas llegan completas.
- El SSIS carga cada lote con un upsert por la llave primaria de cada tabla. Si una misma llave llega más de una vez, prevalece la fila con la `fecha_actualizacion` más reciente.
- Los archivos están en UTF-8, separados por comas y con encabezado. Los campos vacíos equivalen a NULL.

Para resolver el reto no hace falta instalar SQL Server: el código del SP y sus salidas son suficientes. Quien quiera ejecutarlo puede usar el [Docker opcional](legado/docker/README.md), que levanta un SQL Server con el lote 1 ya cargado.

## 3. Criterio de aceptación

La migración se considera aceptada cuando el reporte nuevo coincide con el actual, o cuando cada diferencia está explicada de forma que Control de Gestión pueda aprobarla.

No se acepta cambiar números del reporte sin informarlo al usuario, aunque el valor nuevo sea el correcto.

## 4. Entregables

La entrega es un repositorio Git con el contenido que se describe a continuación.

### 4.1 Pipeline en PySpark sobre Delta

El pipeline carga el lote 1, genera el reporte y luego incorpora el lote 2 mediante MERGE. Volver a ejecutar un lote no debe alterar los resultados. El procesamiento se hace en Spark (DataFrame API o Spark SQL), no en pandas ni en DuckDB.

El punto de entrada debe funcionar en local y estar preparado para ejecutarse como job de Glue, con las rutas recibidas por parámetro, ya sean locales o `s3://`. Se esperan, como mínimo, pruebas que verifiquen que el reporte coincide con la salida de SQL Server y que una segunda ejecución del mismo lote no cambia nada.

### 4.2 Contrato de salida

El reporte se entrega en CSV, con las mismas columnas y en el mismo orden que `rpt.margen_mensual`:

```
mes,id_tienda,categoria,unidades,tickets,unidades_por_ticket,venta_neta,costo,margen,margen_pct
```

- `salida/rpt_margen_mensual_lote1.csv`: el reporte después de cargar el lote 1.
- `salida/rpt_margen_mensual_lote2.csv`: el reporte después de cargar el lote 2.
- `salida/rpt_margen_mensual_lote2_corregido.csv`: solo si se implementan correcciones, que van en un archivo aparte.

El campo `mes` va en formato `AAAA-MM-DD`, los decimales con punto y sin separador de miles, y los NULL como campo vacío.

### 4.3 `CONCILIACION.md`

La comparación contra la salida de SQL Server y, de manera opcional, contra el registro contable. Cada diferencia debe quedar explicada y expresada en soles.

### 4.4 `NOTA_USUARIO.md`

Una nota de una página como máximo, dirigida a la jefa de Control de Gestión, que no tiene perfil técnico. Debe explicar qué cambia en el reporte nuevo, en qué magnitud, por qué, y qué decisión se necesita de su parte.

### 4.5 `DESPLIEGUE_GLUE.md`

Una nota de una página como máximo sobre cómo llevar el pipeline a producción en AWS Glue. Debe cubrir al menos la versión y el tipo de job, los workers, la configuración de Delta, la estructura en S3 y el catálogo, la orquestación y la estrategia incremental, el monitoreo y las alertas, el tratamiento de los datos personales y un costo aproximado.

### 4.6 `AI_LOG.md`

El uso de asistentes de IA está permitido; lo que interesa es ver con qué criterio se usan. Esta bitácora es obligatoria y debe incluir:

1. Las herramientas y los modelos utilizados (Claude, ChatGPT, Copilot, Cursor, agentes, MCP u otros), con su versión cuando se conozca.
2. El flujo de trabajo: si se usó un solo agente o varios, plan mode o MCP, y cómo se coordinaron.
3. Entre tres y cinco prompts clave, con lo que se pidió, un resumen de la respuesta y qué se hizo con ella (aceptarla, modificarla o descartarla), y por qué.
4. Al menos un caso en que la IA se equivocó o propuso algo mejorable, y cómo se detectó y se corrigió.
5. Una autocrítica: qué parte del trabajo es propia, qué parte se debe a la IA y cómo se validó que el resultado es correcto, más allá de que se ejecute sin errores.

No hay una cantidad de IA correcta: se valora el criterio con que se usa, no cuánto se usa.

### 4.7 README

Las instrucciones para ejecutar el pipeline lote por lote, las decisiones y supuestos adoptados, y lo que quedó fuera del alcance, con el motivo.

## 5. Entorno

La referencia es AWS Glue 5.1, la versión por defecto para jobs nuevos: Spark 3.5.6, Python 3.11, Delta Lake 3.3.2 y Java 17.

Para trabajar en local se necesitan Python 3.11 y Java 17, con `JAVA_HOME` apuntando a Java 17:

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install pyspark==3.5.6 delta-spark==3.3.2
```

```python
import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)   # los workers usan este mismo Python

builder = (
    SparkSession.builder.appName("cafe-altura").master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

- En Windows se recomienda WSL2 (Ubuntu), porque Spark nativo en Windows necesita `winutils` para escribir archivos.
- También se acepta Glue 6.0 (Spark 4.1.1, Python 3.13, Delta 4.2.0), siempre que se indique en el README.
- No se requiere una cuenta de AWS.

## 6. Prioridades

Se prefiere un alcance menor bien resuelto. Si el tiempo no alcanza, conviene avanzar en este orden e indicar en el README qué quedó pendiente:

1. El reporte del lote 1 cuadrado con SQL Server, o sus diferencias explicadas.
2. La carga del lote 2 con MERGE.
3. La conciliación y la nota a Control de Gestión.
4. La nota de despliegue en Glue.
5. Pruebas adicionales.

## 7. Sesión de entrevista

Después de la entrega se agenda una sesión para revisar la solución. No hace falta preparar diapositivas. La sesión incluye:

- Una demostración del pipeline con los lotes 1 y 2, y de la conciliación.
- Una incidencia en vivo: un lote nuevo y el mensaje de un usuario, que se resuelven sobre el propio pipeline. En esa parte se puede usar IA con la pantalla compartida.
- Una conversación con Control de Gestión para explicar las diferencias a una persona sin perfil técnico.
- La revisión del `AI_LOG.md`, a partir de una decisión concreta sobre algo que se aceptó o se descartó de la IA, y de cómo se validó.
- Preguntas sobre Glue y sobre cómo llevar la solución a producción.

## 8. Criterios de evaluación

| Dimensión | Peso |
|---|---|
| Paridad y conciliación con SQL Server | 20% |
| Carga incremental con Delta | 15% |
| Criterio sobre el proceso actual | 15% |
| Código, tests y preparación para Glue | 10% |
| Incidencia en vivo (en la sesión) | 15% |
| Comunicación con el usuario de negocio (nota y sesión) | 10% |
| Uso de IA (`AI_LOG.md` y defensa) | 10% |
| Nota de despliegue en Glue | 5% |

## 9. Entrega

Al vencer el plazo, o antes si el trabajo está terminado, se envía por correo al reclutador:

1. El enlace al repositorio Git, público o privado con acceso para el equipo de Tuxpas.
2. Un párrafo breve en el cuerpo del correo con lo que se logró, lo que quedó fuera y por qué.

El `AI_LOG.md`, la conciliación, las dos notas y los CSV de `salida/` deben estar dentro del repositorio.

## Preguntas frecuentes

**¿Hace falta instalar SQL Server?** No. El Docker es opcional y sirve solo para ejecutar el SP.

**¿Se puede usar pandas o DuckDB?** Para explorar los datos, sí. El pipeline tiene que procesar en Spark.

**¿Se puede usar IA?** Sí, siempre que quede documentado en el `AI_LOG.md`. En la sesión se conversa sobre todo lo que contiene el repositorio, así que conviene entender cada parte de lo entregado.

**¿Qué pasa si no se termina todo?** Conviene seguir el orden de la sección 6 e indicar en el README qué quedó pendiente.

Cualquier duda sobre el enunciado se puede consultar con el reclutador.

— Equipo Tuxpas
