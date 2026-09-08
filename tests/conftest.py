"""Shared pytest fixtures for RetailDW local tests."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("retaildw-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def repo_root() -> Path:
    return ROOT


@pytest.fixture
def good_ods() -> Path:
    return ROOT / "tests" / "fixtures" / "good"


@pytest.fixture
def bad_ods() -> Path:
    return ROOT / "tests" / "fixtures" / "bad_quality"


@pytest.fixture
def bad_fk_ods() -> Path:
    return ROOT / "tests" / "fixtures" / "bad_fk"


@pytest.fixture
def bad_amount_ods() -> Path:
    return ROOT / "tests" / "fixtures" / "bad_amount"


@pytest.fixture
def window_ods() -> Path:
    return ROOT / "tests" / "fixtures" / "window"