/* Carga el lote 1 en stg, crea el SP y arma el reporte.
   Lo ejecuta init/entrypoint.sh al levantar el contenedor, con los CSV ya convertidos
   a Windows-1252 en /tmp/carga/lote_1 (init/preparar_csv.sh). */
:r /legado/ddl.sql

USE CafeAltura;
GO

BULK INSERT stg.ventas_pos       FROM '/tmp/carga/lote_1/ventas_pos.csv'       WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = 'RAW', ROWTERMINATOR = '0x0a', KEEPNULLS, TABLOCK);
BULK INSERT stg.pedidos_web      FROM '/tmp/carga/lote_1/pedidos_web.csv'      WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = 'RAW', ROWTERMINATOR = '0x0a', KEEPNULLS, TABLOCK);
BULK INSERT stg.mapeo_sku        FROM '/tmp/carga/lote_1/mapeo_sku.csv'        WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = 'RAW', ROWTERMINATOR = '0x0a', KEEPNULLS, TABLOCK);
BULK INSERT stg.productos        FROM '/tmp/carga/lote_1/productos.csv'        WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = 'RAW', ROWTERMINATOR = '0x0a', KEEPNULLS, TABLOCK);
BULK INSERT stg.costos_historico FROM '/tmp/carga/lote_1/costos_historico.csv' WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = 'RAW', ROWTERMINATOR = '0x0a', KEEPNULLS, TABLOCK);
BULK INSERT stg.tipo_cambio      FROM '/tmp/carga/lote_1/tipo_cambio.csv'      WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = 'RAW', ROWTERMINATOR = '0x0a', KEEPNULLS, TABLOCK);
BULK INSERT stg.tiendas          FROM '/tmp/carga/lote_1/tiendas.csv'          WITH (FORMAT = 'CSV', FIRSTROW = 2, CODEPAGE = 'RAW', ROWTERMINATOR = '0x0a', KEEPNULLS, TABLOCK);
GO

:r /legado/usp_margen_mensual.sql

EXEC rpt.usp_margen_mensual;
GO
