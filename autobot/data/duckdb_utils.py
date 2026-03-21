"""DuckDB helpers for safe temp directory settings."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


DEFAULT_DUCKDB_TEMP_DIRECTORY = "D:/MyApps/Autobot/data/cache/duckdb_tmp"


@dataclass(frozen=True)
class DuckDBSettings:
    temp_directory: str | None = DEFAULT_DUCKDB_TEMP_DIRECTORY
    memory_limit: str = "6GB"
    threads: int = 2
    fail_if_temp_not_set: bool = True


def create_duckdb_connection(settings: DuckDBSettings) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with required temp safety pragmas."""

    temp_directory = (settings.temp_directory or "").strip()
    if settings.fail_if_temp_not_set and not temp_directory:
        raise ValueError("duckdb.temp_directory must be configured")

    if not temp_directory:
        temp_directory = DEFAULT_DUCKDB_TEMP_DIRECTORY

    temp_path = Path(temp_directory).expanduser().resolve()
    temp_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    escaped_temp = temp_path.as_posix().replace("'", "''")
    escaped_mem = settings.memory_limit.replace("'", "''")
    con.execute(f"PRAGMA temp_directory='{escaped_temp}';")
    con.execute(f"PRAGMA memory_limit='{escaped_mem}';")
    con.execute(f"PRAGMA threads={int(settings.threads)};")
    return con
