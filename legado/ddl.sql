/* =====================================================================
   Café Altura — esquema del proceso actual en SQL Server
   ---------------------------------------------------------------------
   stg  Tablas que carga el paquete SSIS cada noche desde el POS, la
        tienda web y el ERP. Carga cada lote con upsert por la llave
        primaria; si una llave llega más de una vez, gana la fila con
        mayor fecha_actualizacion.
   rpt  El reporte de margen mensual que arma rpt.usp_margen_mensual.
   ===================================================================== */

IF DB_ID('CafeAltura') IS NULL
    CREATE DATABASE CafeAltura COLLATE Modern_Spanish_CI_AS;
GO

USE CafeAltura;
GO

CREATE SCHEMA stg;
GO

CREATE SCHEMA rpt;
GO

-- Líneas de comprobantes SUNAT emitidos en las tiendas:
-- 01 factura, 03 boleta, 07 nota de crédito (comprobante_ref = comprobante que afecta).
-- estado: EMITIDO o ANULADO (comunicación de baja).
CREATE TABLE stg.ventas_pos (
    tipo_comprobante    CHAR(2)        NOT NULL,
    serie               CHAR(4)        NOT NULL,
    numero              INT            NOT NULL,
    item                SMALLINT       NOT NULL,
    fecha_emision       DATETIME2(0)   NOT NULL,
    id_tienda           VARCHAR(4)     NOT NULL,
    sku                 VARCHAR(20)    NOT NULL,
    descripcion         VARCHAR(80)    NOT NULL,
    cantidad            INT            NOT NULL,
    importe_total       DECIMAL(12,2)  NOT NULL,   -- con IGV, en soles
    estado              VARCHAR(10)    NULL,
    comprobante_ref     VARCHAR(15)    NULL,
    fecha_actualizacion DATETIME2(0)   NOT NULL,
    CONSTRAINT pk_ventas_pos PRIMARY KEY (tipo_comprobante, serie, numero, item)
);

-- Pedidos de la tienda web. Todos se envían dentro del Perú; algunos se pagan en dólares.
CREATE TABLE stg.pedidos_web (
    id_pedido           VARCHAR(10)    NOT NULL,
    linea               SMALLINT       NOT NULL,
    fecha_pedido        DATETIME2(0)   NOT NULL,
    sku_web             VARCHAR(60)    NOT NULL,
    cantidad            INT            NOT NULL,
    importe             DECIMAL(12,2)  NOT NULL,   -- con IGV, en la moneda del pedido
    moneda              CHAR(3)        NOT NULL,   -- PEN o USD
    estado              VARCHAR(12)    NOT NULL,   -- PAGADO, CANCELADO o REEMBOLSADO
    cliente_email       VARCHAR(120)   NOT NULL,
    cliente_doc         VARCHAR(11)    NOT NULL,   -- DNI o RUC
    fecha_actualizacion DATETIME2(0)   NOT NULL,
    CONSTRAINT pk_pedidos_web PRIMARY KEY (id_pedido, linea)
);

-- Equivalencias de códigos: POS, ERP y tienda web.
CREATE TABLE stg.mapeo_sku (
    sku_pos             VARCHAR(20)    NOT NULL,
    sku_erp             VARCHAR(20)    NULL,
    sku_web             VARCHAR(60)    NULL,
    CONSTRAINT pk_mapeo_sku PRIMARY KEY (sku_pos)
);

-- Maestro de productos del ERP.
CREATE TABLE stg.productos (
    sku_erp             VARCHAR(20)    NOT NULL,
    nombre              VARCHAR(80)    NOT NULL,
    categoria           VARCHAR(20)    NOT NULL,
    CONSTRAINT pk_productos PRIMARY KEY (sku_erp)
);

-- Costo unitario sin IGV. vigente_hasta es el último día en que aplica; NULL = vigente hoy.
CREATE TABLE stg.costos_historico (
    sku_erp             VARCHAR(20)    NOT NULL,
    vigente_desde       DATE           NOT NULL,
    vigente_hasta       DATE           NULL,
    costo_unitario      DECIMAL(12,4)  NOT NULL,
    CONSTRAINT pk_costos_historico PRIMARY KEY (sku_erp, vigente_desde)
);

-- Tipo de cambio SBS (soles por dólar) de cada día publicado.
CREATE TABLE stg.tipo_cambio (
    fecha               DATE           NOT NULL,
    tc                  DECIMAL(6,3)   NOT NULL,
    CONSTRAINT pk_tipo_cambio PRIMARY KEY (fecha)
);

CREATE TABLE stg.tiendas (
    id_tienda           VARCHAR(4)     NOT NULL,
    nombre              VARCHAR(60)    NOT NULL,
    ciudad              VARCHAR(30)    NOT NULL,
    fecha_apertura      DATE           NOT NULL,
    CONSTRAINT pk_tiendas PRIMARY KEY (id_tienda)
);

-- Reporte de margen mensual por tienda y categoría (la web aparece como tienda 'WEB').
CREATE TABLE rpt.margen_mensual (
    mes                 DATE           NOT NULL,
    id_tienda           VARCHAR(4)     NOT NULL,
    categoria           VARCHAR(20)    NOT NULL,
    unidades            INT            NOT NULL,
    tickets             INT            NOT NULL,
    unidades_por_ticket INT            NULL,
    venta_neta          DECIMAL(14,2)  NULL,
    costo               DECIMAL(14,2)  NULL,
    margen              DECIMAL(14,2)  NULL,
    margen_pct          DECIMAL(9,2)   NULL,
    CONSTRAINT pk_margen_mensual PRIMARY KEY (mes, id_tienda, categoria)
);
GO
