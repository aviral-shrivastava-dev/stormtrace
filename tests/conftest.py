"""Shared pytest configuration.

src/ is a directory of scripts rather than an installed package, so it is
put on sys.path here. This mirrors how the orchestrator runs each step
(`python src/<script>.py` with the project root as the working directory).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture
def duckdb_connection():
    """An in-memory DuckDB connection, closed after the test."""
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def project_root() -> Path:
    return ROOT
