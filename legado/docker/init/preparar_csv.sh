#!/bin/bash
# Convierte los CSV de un lote (UTF-8) a Windows-1252, la página de códigos de la collation
# Modern_Spanish_CI_AS, para que BULK INSERT cargue bien los acentos.
# Uso: preparar_csv.sh lote_1   (deja los archivos en /tmp/carga/lote_1/)
set -euo pipefail
mkdir -p "/tmp/carga/$1"
for archivo in /datos/"$1"/*.csv; do
  iconv -f UTF-8 -t WINDOWS-1252 "$archivo" > "/tmp/carga/$1/$(basename "$archivo")"
done
