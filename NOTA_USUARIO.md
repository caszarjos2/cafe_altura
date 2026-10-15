# Nota para Control de Gestión: reporte de margen mensual

**Asunto:** el reporte nuevo y por qué nunca cuadró con Contabilidad

## 1. El reporte nuevo da lo mismo que el de hoy

Trasladamos el reporte a la nueva plataforma en AWS. Con los mismos datos, entrega **exactamente las mismas cifras** que el reporte actual: comparamos todas las filas de enero 2025 a septiembre 2026 y no hay ni un céntimo de diferencia. El cambio de plataforma no modifica ningún número.

## 2. Por qué nunca cuadró con Contabilidad

Encontramos la causa. El reporte actual **deja fuera ventas y calcula mal parte del costo**. Entre enero 2025 y agosto 2026 le faltan **S/ 457,788 de venta (9.2%)**. Hay seis motivos:

| Motivo | Efecto en el reporte actual |
|---|---|
| Siete productos de tienda y web no estaban registrados en la tabla de equivalencias de códigos (por ejemplo, el Latte grande) y sus ventas se descartaban | Faltan S/ 255,440 de venta |
| Boletas válidas que llegan sin estado se descartaban por error | Faltan S/ 88,188 de venta |
| El delivery se excluye del reporte, pero Contabilidad sí lo registra como venta | Faltan S/ 65,100 de venta |
| Los pedidos web en dólares de fines de semana y feriados no se convertían a soles, porque ese día no hay tipo de cambio publicado | Faltan S/ 49,060 de venta |
| Todas las ventas, también las de 2025, se valorizan con el costo de **hoy** y no con el de la fecha en que se vendieron | El costo de meses pasados queda inflado |
| Dos productos que ya no se venden (Chemex y Tartaleta de fresa) aparecen con costo cero | Su margen se ve inflado |

Con estos seis puntos corregidos, el reporte **cuadra al céntimo con Contabilidad** en los 20 meses y en los dos canales (tiendas y web).

Hay un efecto más que conviene conocer. Como el costo se toma "de hoy", **los meses ya cerrados cambian cada vez que sube un costo**. Por ejemplo, al cargar los datos de septiembre, el margen de enero 2025 bajó, aunque esas ventas no cambiaron.

## 3. Cuánto cambian las cifras

| Enero 2025 a agosto 2026 | Reporte actual | Reporte corregido |
|---|---:|---:|
| Venta neta | S/ 4,506,566 | S/ 4,964,354 |
| Margen | S/ 2,225,541 | S/ 2,639,255 |
| Margen % | 49.4% | 53.2% |

El margen real es unos **4 puntos más alto** de lo que muestra el reporte actual. Las cifras corregidas están en un archivo aparte (`rpt_margen_mensual_lote2_corregido.csv`), **no reemplazan al reporte actual** hasta que usted lo apruebe.

## 4. Qué necesitamos de usted

1. **¿Aprueba el reporte corregido como reporte oficial?** Nuestra recomendación es sí: es el que coincide con Contabilidad.
2. **Delivery:** ¿lo incluimos en el reporte? Si se incluye, aparece como categoría aparte ("servicio") y se puede filtrar. Si no, el reporte seguirá sin cuadrar con Contabilidad por unos S/ 65 mil. Recomendamos incluirlo.
3. **Costo por fecha de venta:** ¿confirma que cada mes se valorice con el costo vigente en ese mes? Así los meses cerrados ya no cambian. Recomendamos que sí.
4. **Equivalencias de códigos:** pedir a TI que registre los siete productos faltantes en el sistema. Mientras tanto, el reporte nuevo los reconoce por su nombre.

El detalle cifra por cifra está en `CONCILIACION.md`. Quedamos atentos para revisarlo juntos.
