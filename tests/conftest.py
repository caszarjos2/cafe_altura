import pytest

from migracion.utils.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
