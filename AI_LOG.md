# AI_LOG — Bitácora de uso de IA

> Documento vivo: se completa a medida que avanza el proyecto.

## 1. Herramientas y modelos

| Herramienta | Modelo / versión | Uso |
|---|---|---|
| Claude Code (extensión de VS Code) | Claude Opus 5.5 (`claude-opus-5-5`) | Lectura del enunciado, exploración de datos, diseño de la arquitectura, código y documentación |

## 2. Flujo de trabajo

- Un solo agente (Claude Code) en modo conversacional dentro del IDE. Sin subagentes, sin MCP y sin plan mode.
- La IA ejecuta comandos y scripts en local (DuckDB y Python) y muestra los resultados. Las decisiones de arquitectura y de alcance las tomo yo; cuando la IA propone algo, lo reviso antes de aceptarlo.
- Orden seguido: leer el enunciado → explorar las fuentes con DuckDB (permitido para explorar) → entender por qué no cuadra con contabilidad → definir la arquitectura → implementar en PySpark + Delta.

## 3. Prompts clave

### 3.1 Comparar el reporte del SP con el registro contable
- **Pedido:** validar primero el rango de fechas de ambas fuentes y luego comparar venta neta y costo por mes y canal.
- **Respuesta:** script DuckDB. Los dos tienen los mismos 20 meses (ene-2025 a ago-2026). El reporte queda por debajo de contabilidad todos los meses: S/ 456,183 en venta (9.2%) y S/ 59,031 en costo.
- **Qué hice:** lo acepté. El patrón (siempre falta y en montos parejos) me llevó a la hipótesis de que el SP **pierde registros**.

### 3.2 ¿Estamos perdiendo registros?
- **Pedido:** confirmar si el SP deja filas fuera.
- **Respuesta:** clasificó cada línea de origen según el motivo por el que entra o se cae. Las líneas que entran suman exactamente lo mismo que el reporte del SP, así que la clasificación es correcta. Las que se caen explican casi toda la brecha: SKU sin mapeo, `estado` NULL, delivery excluido y pedidos USD sin tipo de cambio.
- **Qué hice:** lo acepté y luego pedí probar las correcciones contra contabilidad. Con 6 ajustes y los datos de lote 1 + lote 2, cuadran **40 de 40** meses y canales al céntimo.

### 3.3 Arquitectura medallion y carga incremental
- **Pedido:** propuse landing (CSV) → bronze (Delta) → silver (tipos, dedup, particiones) → gold (reporte), con 3 jobs.
- **Respuesta:** la IA estuvo de acuerdo y propuso bronze como `append` con `replaceWhere` por lote, y MERGE en silver.
- **Qué hice:** **modifiqué** la propuesta para bronze. Como landing ya guarda el histórico por lote, bronze puede ser overwrite por tabla con columnas de auditoría (`_lote`, `_archivo`, `_fecha_ingesta`), que es más simple e igual de idempotente. Para silver aclaré que lo que quiero es un upsert por PK (SCD Tipo 1), y lo mantuvimos con MERGE porque el lote 2 de `ventas_pos` y `pedidos_web` es incremental.

## 4. Casos en que la IA se equivocó o propuso algo mejorable

### 4.1 Se pasó del alcance pedido
- **Qué pasó:** le pedí código de exploración para comparar contra contabilidad. La IA creó un módulo completo que replicaba el SP y varios scripts más.
- **Cómo lo detecté:** no era lo que había pedido y no lo entendía paso a paso.
- **Corrección:** le pedí borrarlo y generar solo la comparación. Seguí con scripts chicos y entendibles.

### 4.2 Montos con error de punto flotante
- **Qué pasó:** la primera agrupación con DuckDB mostró montos como `205753.08000000005`, porque `read_csv` infiere `DOUBLE`.
- **Cómo lo detecté:** revisando la salida.
- **Corrección:** cast explícito a `DECIMAL(14,2)`, como el DDL. Lección para el pipeline: leer con esquema explícito, nunca con tipos inferidos.

### 4.3 Bronze sobre-diseñado
- Ver 3.3: el `replaceWhere` por lote era innecesario dado que landing conserva el histórico.

## 5. Hallazgos validados (no asumidos)

- **Collation de SQL Server** (`Modern_Spanish_CI_AS`): compara sin distinguir mayúsculas e ignora espacios finales. Las tiendas de Cusco envían SKU como `'ca-00024 '` y la categoría delivery es `'servicio'` en minúsculas; SQL Server los trata igual que `'CA-00024'` y `'SERVICIO'`. Spark no, así que la réplica debe normalizar. Se detectó porque la réplica no emparejaba filas con la salida real del SP.
- **Lote 2 trae llaves duplicadas** dentro del archivo: 240 en POS y 49 en web. Unas son cambios de estado (gana la `fecha_actualizacion` más reciente) y otras son duplicados exactos (se verificó que todas las columnas son idénticas). Hay que deduplicar antes del MERGE.
- **No hay llaves subrogadas:** todas las PK son naturales, según el DDL, y no tienen NULL.

## 6. Autocrítica

_Pendiente: se completa al cierre del proyecto (qué es propio, qué es de la IA y cómo se validó)._
