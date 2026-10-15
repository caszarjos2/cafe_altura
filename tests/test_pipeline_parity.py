"""End-to-end: run the real batches through landing -> bronze -> silver -> gold and compare
the report with the output SQL Server produced (legado/salida). Also checks idempotency."""
import filecmp
import subprocess
from pathlib import Path

import pytest

from migracion.jobs.bronze_to_silver import process_table
from migracion.jobs.landing_to_bronze import read_landing, write_bronze
from migracion.reports.legacy_margin import build_legacy_margin
from migracion.utils.csv_export import write_single_csv
from migracion.utils.tables import TABLES

ROOT = Path(__file__).resolve().parents[1]
BATCHES = [("lote_1", "2026-09-01", "rpt_margen_mensual_lote1.csv"),
           ("lote_2", "2026-09-15", "rpt_margen_mensual_lote2.csv")]


def run_batch(spark, lake: Path, source: str, load_date: str, output: Path) -> None:
    subprocess.run(
        ["bash", str(ROOT / "migracion/scripts/upload_landing.sh"),
         str(ROOT / "datos" / source), load_date, str(lake / "landing")],
        check=True, capture_output=True,
    )
    for table in TABLES:
        write_bronze(read_landing(spark, str(lake / "landing"), table, load_date), str(lake / "bronze"), table)
        process_table(spark, str(lake / "bronze"), str(lake / "silver"), table)
    report = build_legacy_margin(spark, str(lake / "silver"))
    write_single_csv(spark, report.orderBy("mes", "id_tienda", "categoria"), str(output))


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    return tmp_path_factory.mktemp("lake")


@pytest.mark.parametrize("source, load_date, expected", BATCHES)
def test_report_matches_sql_server(spark, lake, source, load_date, expected):
    """Batches run in order (lote_1 then lote_2) on the same lake, like production."""
    output = lake / f"out_{expected}"
    run_batch(spark, lake, source, load_date, output)
    assert filecmp.cmp(output, ROOT / "legado/salida" / expected, shallow=False)


def test_rerunning_last_batch_changes_nothing(spark, lake):
    source, load_date, expected = BATCHES[-1]
    output = lake / "rerun.csv"
    run_batch(spark, lake, source, load_date, output)
    assert filecmp.cmp(output, ROOT / "legado/salida" / expected, shallow=False)
