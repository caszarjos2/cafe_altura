#!/usr/bin/env bash
# Runs the whole pipeline for one batch: landing -> bronze -> silver -> gold (+ CSV export).
#
# Usage:  run_batch.sh <source_dir> <load_date YYYY-MM-DD> <output_csv> [corrected_csv] [lake_path]
# e.g.:   run_batch.sh datos/lote_1 2026-09-01 salida/rpt_margen_mensual_lote1.csv
#         run_batch.sh datos/lote_2 2026-09-15 salida/rpt_margen_mensual_lote2.csv \
#                      salida/rpt_margen_mensual_lote2_corregido.csv
#
# corrected_csv is optional (empty = do not export the corrected report).
# lake_path defaults to migracion/data (local); it can be an s3:// prefix.
set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "Usage: $0 <source_dir> <load_date YYYY-MM-DD> <output_csv> [corrected_csv] [lake_path]" >&2
    exit 1
fi
SOURCE_DIR="$1"
LOAD_DATE="$2"
OUTPUT_CSV="$3"
CORRECTED_CSV="${4:-}"
LAKE="${5:-migracion/data}"

bash "$(dirname "$0")/upload_landing.sh" "$SOURCE_DIR" "$LOAD_DATE" "$LAKE/0_landing"

python -m migracion.jobs.landing_to_bronze \
    --landing_path "$LAKE/0_landing" --bronze_path "$LAKE/1_bronze" --load_date "$LOAD_DATE"

python -m migracion.jobs.bronze_to_silver \
    --bronze_path "$LAKE/1_bronze" --silver_path "$LAKE/2_silver"

python -m migracion.jobs.silver_to_gold \
    --silver_path "$LAKE/2_silver" --gold_path "$LAKE/3_gold" \
    --output_file "$OUTPUT_CSV" --corrected_output_file "$CORRECTED_CSV"
