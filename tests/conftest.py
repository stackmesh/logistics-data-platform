from __future__ import annotations

import pytest

from etl.config import DateWindow, load_config, source_engine, warehouse_engine


@pytest.fixture(scope="session")
def etl_config():
    return load_config()


@pytest.fixture(scope="session")
def dwh_engine(etl_config):
    engine = warehouse_engine(etl_config)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def src_engine(etl_config):
    engine = source_engine(etl_config)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def loaded_warehouse(etl_config):
    """One committed full load so every integration test starts from the complete dataset."""
    from etl.pipeline.runner import run_pipeline

    return run_pipeline(DateWindow(), etl_config)


@pytest.fixture
def dwh_tx(dwh_engine):
    """A warehouse connection whose work is always rolled back."""
    connection = dwh_engine.connect()
    transaction = connection.begin()
    try:
        yield connection
    finally:
        if transaction.is_active:
            transaction.rollback()
        connection.close()
