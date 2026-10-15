#!/usr/bin/env bash
# Sube los CSV de un lote a la capa landing, particionados por fecha de carga.
#
# Uso:   subir_landing.sh <carpeta_origen> <fecha_carga AAAA-MM-DD> <ruta_landing>
# Ej.:   subir_landing.sh datos/lote_1 2026-09-01 migracion/data/0_landing
#        subir_landing.sh datos/lote_2 2026-09-15 s3://cafe-altura-lake/0_landing
#
# Resultado: <ruta_landing>/<tabla>/fecha_carga=<fecha>/<tabla>.csv
# Volver a subir la misma fecha reemplaza los archivos (idempotente).
set -euo pipefail

TABLAS=(ventas_pos pedidos_web mapeo_sku productos costos_historico tipo_cambio tiendas)

if [[ $# -ne 3 ]]; then
    echo "Uso: $0 <carpeta_origen> <fecha_carga AAAA-MM-DD> <ruta_landing>" >&2
    exit 1
fi
ORIGEN="${1%/}"
FECHA="$2"
DESTINO="${3%/}"

if ! [[ "$FECHA" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! date -d "$FECHA" >/dev/null 2>&1; then
    echo "fecha_carga inválida: '$FECHA' (formato AAAA-MM-DD)" >&2
    exit 1
fi

# Validar que el lote venga completo antes de subir nada
for t in "${TABLAS[@]}"; do
    if [[ ! -s "$ORIGEN/$t.csv" ]]; then
        echo "Falta o está vacío: $ORIGEN/$t.csv" >&2
        exit 1
    fi
done

for t in "${TABLAS[@]}"; do
    destino="$DESTINO/$t/fecha_carga=$FECHA/$t.csv"
    if [[ "$DESTINO" == s3://* ]]; then
        aws s3 cp "$ORIGEN/$t.csv" "$destino" --only-show-errors
    else
        mkdir -p "$(dirname "$destino")"
        cp "$ORIGEN/$t.csv" "$destino"
    fi
    filas=$(( $(wc -l < "$ORIGEN/$t.csv") - 1 ))
    printf '%-18s %8d filas -> %s\n' "$t" "$filas" "$destino"
done
