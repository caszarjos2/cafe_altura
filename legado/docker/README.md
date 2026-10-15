# SQL Server del proceso actual (opcional)

Este entorno no es necesario para resolver el reto: el código del SP y sus salidas ya están en [legado/](../). Sirve para ejecutar el proceso actual y comprobar cómo se comporta.

## Contenido

Un SQL Server 2022 (edición Developer) con la base `CafeAltura`, que incluye:

- las tablas `stg` con el lote 1 cargado;
- el stored procedure `rpt.usp_margen_mensual`;
- el reporte `rpt.margen_mensual` ya generado.

El lote 2 no se carga de forma automática; se puede cargar por cuenta propia para probarlo.

## Requisitos

- Docker con Docker Compose.
- Unos 2 GB de memoria libre.
- En equipos Mac con chip Apple, la emulación x86/amd64 (Rosetta) activada en Docker Desktop, porque la imagen de SQL Server solo existe para amd64.

## Uso

```bash
cd legado/docker
docker compose up -d --wait     # la primera vez descarga la imagen (~1.6 GB) y carga el lote 1
docker exec -it cafe-altura-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'CafeAltura#2026' -C -d CafeAltura
```

Dentro de `sqlcmd`:

```sql
EXEC rpt.usp_margen_mensual;
GO
SELECT TOP 10 * FROM rpt.margen_mensual ORDER BY mes, id_tienda, categoria;
GO
```

También es posible conectarse desde cualquier cliente (DBeaver, la extensión mssql de VS Code u otro) a `localhost,1433`, con el usuario `sa` y la contraseña `CafeAltura#2026`.

Para detener el contenedor y borrar los datos: `docker compose down -v`.

## Carga de los datos

Al arrancar, [init/entrypoint.sh](init/entrypoint.sh) convierte los CSV del lote 1 de UTF-8 a Windows-1252, para que `BULK INSERT` cargue bien los acentos. Luego ejecuta [init/cargar_lote1.sql](init/cargar_lote1.sql), que crea el esquema, carga las tablas, crea el stored procedure y lo ejecuta. Si el contenedor se reinicia, la base ya cargada se conserva.
