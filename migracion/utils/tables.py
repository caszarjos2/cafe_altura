"""Source tables (SQL Server stg schema): primary keys and column types, as defined in legado/ddl.sql."""

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

# DDL types mapped to Spark SQL types (CHAR/VARCHAR -> string, DATETIME2 -> timestamp).
SCHEMAS = {
    "ventas_pos": {
        "tipo_comprobante": "string",
        "serie": "string",
        "numero": "int",
        "item": "smallint",
        "fecha_emision": "timestamp",
        "id_tienda": "string",
        "sku": "string",
        "descripcion": "string",
        "cantidad": "int",
        "importe_total": "decimal(12,2)",
        "estado": "string",
        "comprobante_ref": "string",
        "fecha_actualizacion": "timestamp",
    },
    "pedidos_web": {
        "id_pedido": "string",
        "linea": "smallint",
        "fecha_pedido": "timestamp",
        "sku_web": "string",
        "cantidad": "int",
        "importe": "decimal(12,2)",
        "moneda": "string",
        "estado": "string",
        "cliente_email": "string",
        "cliente_doc": "string",
        "fecha_actualizacion": "timestamp",
    },
    "mapeo_sku": {"sku_pos": "string", "sku_erp": "string", "sku_web": "string"},
    "productos": {"sku_erp": "string", "nombre": "string", "categoria": "string"},
    "costos_historico": {
        "sku_erp": "string",
        "vigente_desde": "date",
        "vigente_hasta": "date",
        "costo_unitario": "decimal(12,4)",
    },
    "tipo_cambio": {"fecha": "date", "tc": "decimal(6,3)"},
    "tiendas": {
        "id_tienda": "string",
        "nombre": "string",
        "ciudad": "string",
        "fecha_apertura": "date",
    },
}

# Fact tables are partitioned by month of the business date; dimensions are not partitioned.
PARTITION_SOURCE = {
    "ventas_pos": "fecha_emision",
    "pedidos_web": "fecha_pedido",
}
PARTITION_COLUMN = "partition_month"

# Column that decides which version of a row is newer (only in fact tables).
VERSION_COLUMN = "fecha_actualizacion"
