"""SCD1 upsert from bronze to silver, tested on small synthetic loads of ventas_pos."""
import datetime as dt

import pytest
from pyspark.sql import functions as F

from migracion.jobs.bronze_to_silver import prepare
from migracion.utils.delta_io import upsert
from migracion.utils.tables import PRIMARY_KEYS, SCHEMAS
from migracion.utils.transforms import cast_to_schema, deduplicate

TABLE = "ventas_pos"


def bronze_rows(spark, rows, load_date):
    """Build a bronze-like DataFrame (all strings + _load_date) from partial ventas_pos rows."""
    base = {
        "tipo_comprobante": "03", "serie": "B001", "numero": "1", "item": "1",
        "fecha_emision": "2026-08-25 09:00:00", "id_tienda": "T001", "sku": "CA-00001",
        "descripcion": "x", "cantidad": "1", "importe_total": "10.00", "estado": "EMITIDO",
        "comprobante_ref": None, "fecha_actualizacion": "2026-08-25 09:05:00",
    }
    data = [tuple({**base, **r}[c] for c in SCHEMAS[TABLE]) for r in rows]
    df = spark.createDataFrame(data, ", ".join(f"{c} string" for c in SCHEMAS[TABLE]))
    return df.withColumn("_load_date", F.lit(load_date).cast("date"))


def load(spark, rows, load_date, path):
    upsert(
        spark,
        prepare(bronze_rows(spark, rows, load_date), TABLE),
        target_path=f"{path}/{TABLE}",
        keys=PRIMARY_KEYS[TABLE],
        compare_columns=list(SCHEMAS[TABLE]),
        version_column="fecha_actualizacion",
        partition_by=["partition_month"],
    )


def snapshot(spark, path):
    return sorted(spark.read.format("delta").load(f"{path}/{TABLE}").drop("partition_month").collect())


def test_dedup_keeps_latest_version(spark):
    df = bronze_rows(spark, [
        {"estado": "EMITIDO", "fecha_actualizacion": "2026-09-01 10:00:00"},
        {"estado": "ANULADO", "fecha_actualizacion": "2026-09-03 17:00:00"},
        {"estado": "ANULADO", "fecha_actualizacion": "2026-09-03 17:00:00"},  # exact duplicate
    ], "2026-09-15")
    typed = cast_to_schema(df, SCHEMAS[TABLE], keep=["_load_date"])
    result = deduplicate(typed, PRIMARY_KEYS[TABLE], order_by="fecha_actualizacion").collect()
    assert len(result) == 1
    assert result[0]["estado"] == "ANULADO"


def test_rerunning_same_load_changes_nothing(spark, tmp_path):
    path = str(tmp_path)
    rows = [{"numero": "1"}, {"numero": "2"}, {"numero": "3", "estado": None}]
    load(spark, rows, "2026-09-01", path)
    first = snapshot(spark, path)
    load(spark, rows, "2026-09-01", path)
    assert snapshot(spark, path) == first


def test_newer_row_updates_and_new_key_inserts(spark, tmp_path):
    path = str(tmp_path)
    load(spark, [{"numero": "1"}], "2026-09-01", path)
    load(spark, [
        {"numero": "1", "estado": "ANULADO", "fecha_actualizacion": "2026-09-02 08:00:00"},
        {"numero": "2", "fecha_emision": "2026-09-10 12:00:00"},
    ], "2026-09-15", path)

    rows = {r["numero"]: r for r in snapshot(spark, path)}
    assert len(rows) == 2
    assert rows[1]["estado"] == "ANULADO"
    assert rows[1]["_load_date"] == dt.date(2026, 9, 15)
    assert rows[2]["_load_date"] == dt.date(2026, 9, 15)


def test_older_row_does_not_overwrite(spark, tmp_path):
    path = str(tmp_path)
    load(spark, [{"estado": "ANULADO", "fecha_actualizacion": "2026-09-03 17:00:00"}], "2026-09-15", path)
    load(spark, [{"estado": "EMITIDO", "fecha_actualizacion": "2026-08-25 09:05:00"}], "2026-09-16", path)

    (row,) = snapshot(spark, path)
    assert row["estado"] == "ANULADO"
    assert row["_load_date"] == dt.date(2026, 9, 15)


def test_unchanged_row_keeps_original_load_date(spark, tmp_path):
    path = str(tmp_path)
    load(spark, [{}], "2026-09-01", path)
    load(spark, [{}], "2026-09-15", path)  # same content arrives again
    (row,) = snapshot(spark, path)
    assert row["_load_date"] == dt.date(2026, 9, 1)


def test_invalid_value_fails_cast(spark):
    df = bronze_rows(spark, [{"importe_total": "diez soles"}], "2026-09-01")
    with pytest.raises(ValueError, match="do not match the expected types"):
        cast_to_schema(df, SCHEMAS[TABLE], keep=["_load_date"])
