"""The corrected report must reconcile with Accounting (datos/referencia/registro_ventas_contable.csv)
once both batches are loaded."""

import pytest
from pyspark.sql import functions as F

from migracion.reports.corrected_margin import build_corrected_lines, build_corrected_margin
from tests.helpers import BATCHES, ROOT, run_batch

def channel():
    return F.when(F.col("id_tienda") == "WEB", "WEB").otherwise("TIENDAS").alias("canal")


@pytest.fixture(scope="module")
def silver(spark, tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake_corrected")
    for source, load_date, _ in BATCHES:
        run_batch(spark, lake, source, load_date, lake / "out.csv")
    return str(lake / "silver")


@pytest.fixture(scope="module")
def accounting(spark):
    return spark.read.option("header", True).csv(str(ROOT / "datos/referencia/registro_ventas_contable.csv")) \
        .selectExpr("CAST(mes AS DATE) mes", "canal",
                    "CAST(venta_neta AS DECIMAL(14,2)) venta_cont", "CAST(costo_ventas AS DECIMAL(14,2)) costo_cont")


def test_corrected_lines_match_accounting_exactly(spark, silver, accounting):
    lines = build_corrected_lines(spark, silver).groupBy("mes", channel()).agg(
        F.sum("venta_sin_igv").alias("venta"), F.sum("costo").cast("decimal(14,2)").alias("costo"))
    joined = accounting.join(lines, ["mes", "canal"], "left")
    assert joined.count() == 40
    assert joined.where("venta_cont = venta AND costo_cont = costo").count() == 40


def test_corrected_report_matches_accounting(spark, silver, accounting):
    """Sales match to the cent; cost only differs by per-row rounding (a few cents)."""
    report = build_corrected_margin(spark, silver)
    assert report.where("categoria IS NULL OR venta_neta IS NULL OR costo IS NULL").count() == 0

    by_channel = report.groupBy("mes", channel()).agg(
        F.sum("venta_neta").alias("venta"), F.sum("costo").alias("costo"))
    joined = accounting.join(by_channel, ["mes", "canal"], "left")
    assert joined.where("venta_cont = venta").count() == 40
    assert joined.where("abs(costo_cont - costo) > 0.50").count() == 0
