# AI_LOG: bitácora de uso de IA

## 1. Herramientas y modelos

| Herramienta | Modelo / versión | Uso |
|---|---|---|
| Claude Code (extensión de VS Code) | Claude Opus 5.5 (`claude-opus-5-5`) | Lectura del enunciado, exploración de datos, código, tests y borradores de documentación |
| DuckDB 1.2.2 (vía la IA) | — | Solo exploración inicial de los CSV (permitido por el enunciado) |

## 2. Flujo de trabajo

- **Un solo agente** (Claude Code) en modo conversacional dentro del IDE. Sin subagentes, sin MCP y sin plan mode. La IA ejecuta comandos en mi máquina (Git Bash y WSL2) y me muestra los resultados.
- **Yo defino la arquitectura y el alcance; la IA implementa y valida.** Trabajé por fases, con un plan de tareas y commits convencionales que acordamos al inicio. Revisé cada fase antes de pasar a la siguiente y pedí cambios cuando algo no me convencía (ver sección 3).
- **Orden:** entender el enunciado → explorar con DuckDB por qué no cuadra con Contabilidad → diseñar la arquitectura medallion → implementar en PySpark + Delta → tests → documentos.
- **Regla de validación:** nada se da por bueno porque "corre". La paridad se compara byte a byte con SQL Server, el cuadre con Contabilidad al céntimo, y silver contra un cálculo independiente en DuckDB.

## 3. Prompts clave

### 3.1 ¿Estamos perdiendo registros?
- **Pedido:** después de comparar el reporte con Contabilidad por mes y canal (el reporte queda por debajo los 40 meses), pregunté si el SP estaba dejando filas fuera.
- **Respuesta:** clasificó cada línea de origen según por qué entra o se cae. Las que entran suman exactamente lo mismo que el reporte del SP, así que la clasificación es correcta, y las que se caen explican casi toda la brecha.
- **Qué hice:** lo acepté. Luego pedí probar las correcciones contra Contabilidad, y con lote 1 + lote 2 cuadraron 40/40. Eso confirmó que el lote 2 funciona como un CDC: trae correcciones de agosto, no solo septiembre.

### 3.2 Arquitectura medallion y carga incremental
- **Pedido:** propuse landing (CSV) → bronze (Delta) → silver (tipos, dedup, particiones) → gold (reporte), con un job por capa.
- **Respuesta:** la IA estuvo de acuerdo, pero propuso bronze como `append` con `replaceWhere` por lote.
- **Qué hice:** **la modifiqué.** Landing ya guarda el histórico, así que bronze puede ser overwrite por carga, que es más simple e igual de idempotente. En silver pedí un upsert por PK al estilo Kimball (SCD Tipo 1) con una sola columna de auditoría (`_load_date`). La IA advirtió que un overwrite en silver habría borrado 20 meses de ventas, porque el lote 2 de `ventas_pos` y `pedidos_web` es incremental; por eso quedó MERGE.

### 3.3 Landing particionado por fecha de carga
- **Pedido:** en vez de `lote_1` y `lote_2`, particionar landing por fecha de carga, y luego ordenar primero por fecha y después por tabla.
- **Respuesta:** la IA derivó las fechas desde los datos (última `fecha_actualizacion` de cada lote: 31-ago y 14-sep, con una carga nocturna al día siguiente) y cambió el script y el job.
- **Qué hice:** lo acepté después de ver la estructura `0_landing/load_date=2026-09-01/<tabla>.csv`. También le pedí que el código esté en inglés y que mueva las funciones reutilizables (cast, dedup, partición, upsert) a `utils/`.

### 3.4 ¿Spark SQL o DataFrame API para el reporte?
- **Pedido:** pregunté si estaba bien escribir el reporte en SQL, siendo una migración a PySpark.
- **Respuesta:** recomendó mantener SQL: queda espejo línea a línea del SP, los `CAST` decimales son los mismos (por eso los redondeos coinciden sin ajustes) y el plan de ejecución es el mismo que con DataFrame API.
- **Qué hice:** lo acepté. Coincidía con mi intuición y el enunciado permite las dos formas.

## 4. Casos en que la IA se equivocó o propuso algo mejorable

### 4.1 Se pasó del alcance pedido
- **Qué pasó:** pedí un script para comparar contra Contabilidad, y la IA creó un módulo completo que replicaba el SP, más varios scripts.
- **Cómo lo detecté:** no era lo que había pedido y no podía seguirlo paso a paso.
- **Corrección:** le pedí borrarlo y generar solo la comparación. Desde ahí trabajé con scripts chicos que entiendo.

### 4.2 Montos con error de punto flotante
- **Qué pasó:** la primera agrupación con DuckDB mostró montos como `205753.08000000005`, porque `read_csv` infiere `DOUBLE`.
- **Corrección:** cast explícito a `DECIMAL(14,2)`. Lección que se aplicó al pipeline: bronze lee todo como texto y silver castea con los tipos exactos del DDL. Si un valor no castea, el job falla.

### 4.3 Bronze sobre-diseñado
- Ver 3.2: el `replaceWhere` por lote era innecesario.

### 4.4 Puente de conciliación incompleto: faltaba una causa
- **Qué pasó:** la primera versión del puente SP → Contabilidad cuadraba en venta, pero en el costo de tiendas le faltaban ~S/ 68 mil para llegar al total corregido.
- **Cómo se detectó:** la IA lo detectó al verificar que el puente sumara. El test del corregido pasaba, así que el error estaba en la explicación, no en el reporte.
- **Causa y corrección:** el efecto "costo de hoy vs costo a la fecha" se calculaba como `costo - costo_hoy`, y para dos productos `costo_hoy` era NULL, así que esas líneas se perdían. Al investigarlo apareció **una causa real nueva**: Chemex y Tartaleta de fresa están descontinuadas y el SP les pone costo cero en todos los meses (S/ 85 mil). Se agregó como causa propia y el puente quedó cuadrando al céntimo.
- **Lección:** que el resultado final cuadre no basta; la explicación también tiene que sumar.

### 4.5 Mapeo por patrón de código vs. por evidencia
- **Qué pasó:** para los productos sin mapeo, la IA propuso primero deducir `ERP-PT-00NN` a partir de `CA-000NN`.
- **Corrección:** antes de implementarlo se verificó en los datos que la `descripcion` de la boleta es idéntica al nombre del producto en el 100% de las líneas mapeadas, y se usó eso, que es una regla respaldada por los datos. Aun así, en la nota se recomienda que TI complete `mapeo_sku`.

### 4.6 Detalles corregidos en la revisión
- La nota para la jefa decía "cinco productos sin mapeo"; son 7 distintos (4 en POS y 5 en web, con 2 repetidos).
- Una cifra de rendimiento en `DESPLIEGUE_GLUE.md` no estaba medida; se reemplazó por una que sí lo está.
- Spark dejaba archivos `.crc` ocultos junto a los CSV de `salida/`; ahora el export los borra.

## 5. Hallazgos validados (no asumidos)

- **Collation de SQL Server** (`Modern_Spanish_CI_AS`): no distingue mayúsculas e ignora los espacios finales. Las tiendas de Cusco envían `'ca-00024 '` y el delivery es `'servicio'` en minúsculas. Se detectó porque la réplica no emparejaba filas con la salida real del SP; sin replicarlo, Spark pierde 3 tiendas y muestra el delivery.
- **El lote 2 trae llaves duplicadas** (240 en POS y 49 en web): unas son cambios de estado y otras duplicados exactos (verificado columna por columna). Se deduplica antes del MERGE.
- **No hay llaves subrogadas:** todas las PK son naturales según el DDL y no tienen NULL.
- **Silver** coincide fila por fila con un upsert calculado de forma independiente en DuckDB: 0 faltantes y 0 sobrantes en las 7 tablas.
- **Paridad byte a byte** (`filecmp`) con `legado/salida` en los dos lotes, y la repetición de un lote no cambia nada.
- **Cuadre con Contabilidad:** 40/40 meses × canal al céntimo a nivel de línea. En el reporte por fila queda un redondeo de S/ 0.09 en total, explicado.

## 6. Autocrítica

**Qué es mío:**
- la lectura del problema: separar paridad con SQL Server de conciliación contable;
- la hipótesis de que el SP perdía registros;
- la arquitectura por capas;
- landing por fecha de carga, bronze en overwrite y upsert SCD1 en silver;
- las convenciones (commits, código en inglés, funciones en `utils/`);
- la priorización por fases y la decisión de qué entra y qué no.

**Qué es de la IA:**
- la mayor parte del código y los tests;
- el análisis fino de los datos: collation, duplicados del lote 2, productos descontinuados;
- los borradores de los documentos, que revisé y ajusté.

**Cómo se validó que es correcto, más allá de que corra:**
- Cada resultado se contrastó contra una fuente externa al código: la salida real de SQL Server (byte a byte), el registro contable (al céntimo) y un cálculo independiente en DuckDB para silver.
- Las reglas nuevas se probaron por contraste. Por ejemplo, sin delivery quedan ~S/ 35 mil de diferencia y con el costo de hoy ~S/ 86 mil, lo que confirma que Contabilidad usa delivery y costo histórico.

**Qué haría distinto:**
- _(completar en primera persona)_
