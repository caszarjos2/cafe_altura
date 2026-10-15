"""End-to-end: run the real batches through landing -> bronze -> silver -> gold and compare
the report with the output SQL Server produced (legado/salida). Also checks idempotency."""
import filecmp

import pytest

from tests.helpers import BATCHES, ROOT, run_batch


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
