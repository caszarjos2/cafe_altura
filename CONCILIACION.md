# Conciliación del reporte de margen mensual

Importes en soles y sin IGV. "SP" es el reporte actual de SQL Server (`rpt.usp_margen_mensual`).

## Resumen

| Comparación | Resultado |
|---|---|
| Spark (réplica) vs SQL Server, lote 1 | **Idéntico** byte a byte: 4,770 filas |
| Spark (réplica) vs SQL Server, lote 2 | **Idéntico** byte a byte: 5,013 filas |
| SP vs Contabilidad (ene-2025 a ago-2026) | Faltan **S/ 457,788.25 de venta** (9.2%) y **S/ 44,074.62 de costo** (1.9%) |
| Reporte corregido vs Contabilidad | **Cuadra 40 de 40** meses × canal, al céntimo, en venta y costo |

La diferencia con Contabilidad no viene de la migración, que reproduce exactamente al SP. Viene de **seis problemas del proceso actual**, explicados en la sección 3.

---

## 1. Paridad con SQL Server

El pipeline (landing → bronze → silver → gold) corre los dos lotes en orden. El reporte que produce se compara **byte a byte** con `legado/salida/`:

- `salida/rpt_margen_mensual_lote1.csv` = `legado/salida/rpt_margen_mensual_lote1.csv`
- `salida/rpt_margen_mensual_lote2.csv` = `legado/salida/rpt_margen_mensual_lote2.csv`

**Diferencia en soles: 0.00** en todas las filas y columnas. Lo verifica `tests/test_pipeline_parity.py`, que además corre el lote 2 dos veces para comprobar que el resultado no cambia.

Para lograr la paridad hubo que replicar un comportamiento implícito de SQL Server. La base usa la collation `Modern_Spanish_CI_AS`, que no distingue mayúsculas e ignora los espacios al final; Spark sí los distingue. Dos casos dependen de esto:

- Las tiendas de Cusco (T032 a T034) envían el SKU como `'ca-00024 '`, en minúsculas y con un espacio al final.
- El ERP guarda la categoría `servicio` en minúsculas, y el SP filtra `<> 'SERVICIO'`.

Sin esa adaptación, el reporte en Spark perdería las tres tiendas de Cusco y mostraría el delivery.

## 2. Qué cambia en el reporte actual al cargar el lote 2

| Tramo | Canal | Venta | Costo | Motivo |
|---|---|---:|---:|---|
| ene-2025 a jul-2026 | TIENDAS | 0.00 | +11,883.83 | Costos nuevos desde el 10-sep-2026 (ver 3.6) |
| ene-2025 a jul-2026 | WEB | 0.00 | +3,070.64 | Ídem |
| ago-2026 | TIENDAS | −1,313.06 | +77.17 | Boletas de agosto anuladas el 1 y 2 de septiembre |
| ago-2026 | WEB | −292.24 | −75.63 | Pedidos de agosto que cambiaron de estado |
| sep-2026 | TIENDAS | +95,601.08 | +48,306.24 | Ventas nuevas del 1 al 14 de septiembre |
| sep-2026 | WEB | +9,824.20 | +6,875.87 | Ídem |

Los cambios de agosto son correctos: es la carga incremental (MERGE) aplicando correcciones tardías.

El cambio de costo en meses ya cerrados **no es correcto**. El SP vuelve a valorizar 20 meses de ventas cada vez que el ERP actualiza un costo. Solo cambiaron 3 productos (café en grano Chanchamayo 250 g, en grano Villa Rica 250 g y molido Chanchamayo 250 g), y el margen de enero de 2025 ya bajó.

## 3. Por qué el reporte nunca cuadró con Contabilidad

Puente del SP (lote 2) a Contabilidad, de ene-2025 a ago-2026. Una línea afectada por varias causas se asigna a la primera, en el orden de la tabla.

| # | Causa | Venta | Costo |
|---|---|---:|---:|
| | **Reporte actual (SP)** | **4,506,565.64** | **2,281,024.62** |
| 3.1 | Ventas con estado vacío descartadas | +88,188.10 | +40,921.61 |
| 3.2 | Productos sin mapeo descartados | +255,440.12 | +98,073.86 |
| 3.3 | Delivery excluido | +65,099.78 | 0.00 |
| 3.4 | Pedidos USD en días sin tipo de cambio | +49,060.25 | 0.00 |
| 3.5 | Productos descontinuados sin costo | 0.00 | +85,035.35 |
| 3.6 | Costo de hoy en vez del costo de la fecha | 0.00 | −179,955.68 |
| 3.7 | Redondeo del costo por fila | 0.00 | −0.52 |
| | **Contabilidad = reporte corregido** | **4,964,353.89** | **2,325,099.24** |

Por canal:

| Causa | TIENDAS venta | TIENDAS costo | WEB venta | WEB costo |
|---|---:|---:|---:|---:|
| Reporte actual (SP) | 4,009,730.85 | 1,957,372.47 | 496,834.79 | 323,652.15 |
| 3.1 Estado vacío | +88,188.10 | +40,921.61 | 0.00 | 0.00 |
| 3.2 Sin mapeo | +210,815.84 | +74,257.78 | +44,624.28 | +23,816.08 |
| 3.3 Delivery | +35,876.50 | 0.00 | +29,223.28 | 0.00 |
| 3.4 USD sin tipo de cambio | 0.00 | 0.00 | +49,060.25 | 0.00 |
| 3.5 Descontinuados | 0.00 | +68,055.05 | 0.00 | +16,980.30 |
| 3.6 Costo a la fecha | 0.00 | −145,463.64 | 0.00 | −34,492.04 |
| 3.7 Redondeo | 0.00 | −0.50 | 0.00 | −0.02 |
| **Contabilidad** | **4,344,611.29** | **1,995,142.77** | **619,742.60** | **329,956.47** |

### 3.1 Ventas con estado vacío (+S/ 88,188 de venta)

El SP filtra `estado <> 'ANULADO'`. En SQL, una comparación con un valor vacío (NULL) no es verdadera, así que **las ventas sin estado también se descartan**. Son boletas y facturas válidas: solo `ANULADO` indica una anulación.

### 3.2 Productos sin mapeo (+S/ 255,440 de venta)

La tabla de equivalencias `mapeo_sku` no tiene todos los productos. Cuando falta uno, la venta se queda sin categoría y el filtro del SP la elimina sin avisar.

- **Tiendas:** `CA-00024` Latte grande (el mayor, ~S/ 122 mil en el lote 1), `CA-00017` Café molido descafeinado 250 g, `CA-00061` Gorra Altura y `CA-00053` Mazamorra morada.
- **Web:** `filtros-de-papel-x100`, `cafe-molido-descafeinado-250-g`, `cafe-en-grano-sandia-250-g`, `gorra-altura` y `bolsa-de-tela-altura`.

Todos existen en el maestro de productos y tienen costo. La corrección los ubica por su nombre:
- en tiendas, la descripción de la boleta es idéntica al nombre del producto (se verificó que coincide en el 100% de las líneas que sí están mapeadas);
- en web, el código es el nombre del producto en minúsculas y con guiones.

### 3.3 Delivery (+S/ 65,100 de venta)

El SP lo excluye a propósito ("el delivery no lleva costo"). Contabilidad lo registra como venta. Tiene costo registrado de S/ 0.00, así que **incluirlo sube la venta y el margen en el mismo monto**. Es una decisión de negocio (ver `NOTA_USUARIO.md`).

### 3.4 Pedidos en dólares sin tipo de cambio (+S/ 49,060 de venta)

El SP convierte con el tipo de cambio **del mismo día**. La SBS no publica los sábados, domingos ni feriados, así que esos pedidos quedan con venta vacía y no se suman. Ocurre en 192 días. La corrección usa el **último tipo de cambio publicado**, como hace Contabilidad.

### 3.5 Productos descontinuados (+S/ 85,035 de costo)

El SP busca solo el costo "vigente hoy". La **Chemex** (costo cerrado el 31-ene-2026) y la **Tartaleta de fresa** (31-may-2026) ya no tienen costo vigente. Por eso el SP les pone **costo cero en todos los meses**, también en los que sí se vendieron. Esto infla su margen.

### 3.6 Costo de hoy vs. costo de la fecha (−S/ 179,956 de costo)

El SP valoriza todas las ventas históricas con el costo actual. Los costos subieron (por ejemplo, el café en grano Chanchamayo pasó de S/ 16.27 a S/ 21.05), así que el SP **sobrevalora el costo de los meses antiguos y subestima su margen**. Además, como muestra la sección 2, los meses ya cerrados cambian cada vez que el ERP actualiza un costo.

### 3.7 Redondeo (−S/ 0.52 de costo)

El reporte redondea el costo a 2 decimales en cada fila (mes × tienda × categoría). Contabilidad redondea el total del mes. La diferencia acumulada es de céntimos, no requiere acción y se mantiene en el reporte corregido (S/ 0.09 en total).

## 4. Reporte corregido

`salida/rpt_margen_mensual_lote2_corregido.csv` aplica las correcciones 3.1 a 3.6 con el mismo formato y las mismas columnas que el reporte actual. El código está en `migracion/reports/corrected_margin.py`, y cada cambio está marcado con `-- CORRECCION n`.

- **Venta:** cuadra al céntimo con Contabilidad en los 40 meses × canal.
- **Costo:** cuadra al céntimo a nivel de detalle. En el reporte queda solo el redondeo por fila de 3.7: entre −S/ 0.12 y +S/ 0.10 por mes, S/ 0.09 en total.
- Hay 468 filas nuevas de categoría `servicio` (delivery).
- Lo verifica `tests/test_corrected_margin.py`.

| Ene-2025 a ago-2026 | Reporte actual | Corregido | Diferencia |
|---|---:|---:|---:|
| Venta neta | 4,506,565.64 | 4,964,353.89 | +457,788.25 |
| Costo | 2,281,024.62 | 2,325,099.33 | +44,074.71 |
| Margen | 2,225,541.02 | 2,639,254.56 | +413,713.54 |
| Margen % | 49.38% | 53.16% | +3.78 pp |

**Septiembre 2026** (no hay registro contable para comparar): el corregido suma S/ +10,675.33 de venta y S/ +3,158.55 de costo frente al SP, por las mismas causas.

## 5. Observaciones que no se corrigieron

- **`unidades_por_ticket` es una división entera:** por ejemplo, 1.9 se muestra como 1. Se mantiene para no cambiar el tipo de la columna del contrato, pero conviene mostrarlo con decimales.
- **El mapeo por nombre es un respaldo.** Lo correcto es que TI complete `mapeo_sku`. Si un producto cambia de nombre, el respaldo deja de funcionar.

## 6. Cómo reproducir

```bash
bash migracion/scripts/run_batch.sh datos/lote_1 2026-09-01 salida/rpt_margen_mensual_lote1.csv
bash migracion/scripts/run_batch.sh datos/lote_2 2026-09-15 salida/rpt_margen_mensual_lote2.csv \
     salida/rpt_margen_mensual_lote2_corregido.csv
python -m migracion.reports.reconciliation --silver_path migracion/data/2_silver \
     --accounting_file datos/referencia/registro_ventas_contable.csv
python -m pytest tests
```
