"""Source tables (SQL Server stg schema) and their primary keys, as defined in legado/ddl.sql."""

PRIMARY_KEYS = {
    "ventas_pos": ["tipo_comprobante", "serie", "numero", "item"],
    "pedidos_web": ["id_pedido", "linea"],
    "mapeo_sku": ["sku_pos"],
    "productos": ["sku_erp"],
    "costos_historico": ["sku_erp", "vigente_desde"],
    "tipo_cambio": ["fecha"],
    "tiendas": ["id_tienda"],
}

TABLES = list(PRIMARY_KEYS)
