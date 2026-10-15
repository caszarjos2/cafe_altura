#!/bin/bash
# Arranca SQL Server, espera a que aplique la collation del servidor y carga el lote 1.
set -uo pipefail
/opt/mssql/bin/sqlservr &
servidor=$!
sqlcmd=(/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b)

for _ in $(seq 1 150); do
  collation=$("${sqlcmd[@]}" -h -1 -W -Q "SET NOCOUNT ON; SELECT CONVERT(varchar(60), SERVERPROPERTY('Collation'))" 2>/dev/null | tr -d '[:space:]')
  [ "$collation" = "$MSSQL_COLLATION" ] && break
  sleep 2
done

existe=$("${sqlcmd[@]}" -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = 'CafeAltura'" 2>/dev/null | tr -d '[:space:]')
if [ "$existe" = "1" ]; then   # reinicio del contenedor: la base ya está cargada
  touch /tmp/cafe_altura_lista
  echo "Café Altura: la base ya estaba cargada; no se vuelve a cargar."
else
  bash /legado/docker/init/preparar_csv.sh lote_1
  if "${sqlcmd[@]}" -i /legado/docker/init/cargar_lote1.sql; then
    touch /tmp/cafe_altura_lista
    echo "Café Altura: lote 1 cargado en stg y reporte rpt.margen_mensual armado."
  else
    echo "Café Altura: falló la carga del lote 1." >&2
  fi
fi
wait "$servidor"
