"""Shared helpers for the end-to-end tests: run real batches through the pipeline."""
import subprocess
from pathlib import Path

from migracion.jobs.bronze_to_silver import process_table
from migracion.jobs.landing_to_bronze import read_landing, write_bronze
from migracion.reports.legacy_margin import build_legacy_margin
from migracion.utils.csv_export import write_single_csv
from migracion.utils.tables import TABLES

ROOT = Path(__file__).resolve().parents[1]
BATCHES = [("lote_1", "2026-09-01", "rpt_margen_mensual_lote1.csv"),
           ("lote_2", "2026-09-15", "rpt_margen_mensual_lote2.csv")]


def run_batch(spark, lake: Path, source: str, load_date: str, output: Path) -> None:
    """landing -> bronze -> silver -> legacy report CSV, on a lake rooted at `lake`."""
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
