#!/usr/bin/env bash
# Uploads one batch of CSV files to the landing layer, partitioned by load date.
#
# Usage:  upload_landing.sh <source_dir> <load_date YYYY-MM-DD> <landing_path>
# e.g.:   upload_landing.sh datos/lote_1 2026-09-01 migracion/data/0_landing
#         upload_landing.sh datos/lote_2 2026-09-15 s3://cafe-altura-lake/0_landing
#
# Result: <landing_path>/load_date=<date>/<table>.csv
# Uploading the same date again replaces the files (idempotent).
set -euo pipefail

TABLES=(ventas_pos pedidos_web mapeo_sku productos costos_historico tipo_cambio tiendas)

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <source_dir> <load_date YYYY-MM-DD> <landing_path>" >&2
    exit 1
fi
SOURCE_DIR="${1%/}"
LOAD_DATE="$2"
TARGET="${3%/}"

if ! [[ "$LOAD_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! date -d "$LOAD_DATE" >/dev/null 2>&1; then
    echo "Invalid load_date: '$LOAD_DATE' (expected YYYY-MM-DD)" >&2
    exit 1
fi

# Make sure the batch is complete before uploading anything
for table in "${TABLES[@]}"; do
    if [[ ! -s "$SOURCE_DIR/$table.csv" ]]; then
        echo "Missing or empty: $SOURCE_DIR/$table.csv" >&2
        exit 1
    fi
done

for table in "${TABLES[@]}"; do
    target_file="$TARGET/load_date=$LOAD_DATE/$table.csv"
    if [[ "$TARGET" == s3://* ]]; then
        aws s3 cp "$SOURCE_DIR/$table.csv" "$target_file" --only-show-errors
    else
        mkdir -p "$(dirname "$target_file")"
        cp "$SOURCE_DIR/$table.csv" "$target_file"
    fi
    rows=$(( $(wc -l < "$SOURCE_DIR/$table.csv") - 1 ))
    printf '%-18s %8d rows -> %s\n' "$table" "$rows" "$target_file"
done
