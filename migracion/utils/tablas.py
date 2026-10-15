"""Tablas del origen (stg de SQL Server) y sus llaves primarias, según legado/ddl.sql."""

PKS = {
    "ventas_pos": ["tipo_comprobante", "serie", "numero", "item"],
    "pedidos_web": ["id_pedido", "linea"],
    "mapeo_sku": ["sku_pos"],
    "productos": ["sku_erp"],
    "costos_historico": ["sku_erp", "vigente_desde"],
    "tipo_cambio": ["fecha"],
    "tiendas": ["id_tienda"],
}

TABLAS = list(PKS)
