"""Data layer for CSV to Parquet ingestion."""

from .column_mapper import ColumnMapping, ColumnMappingError, detect_column_mapping
from .duckdb_utils import DuckDBSettings, create_duckdb_connection
from .filename_parser import FilenameParseError, parse_upbit_filename
from .ingest_csv_to_parquet import IngestOptions, ingest_dataset, sniff_csv_files, validate_dataset
from .inventory import build_candle_inventory, default_inventory_window, estimate_recent_value_by_market, parse_utc_ts_ms
from .manifest import build_manifest_index, load_manifest, manifest_path, should_skip_file
from .schema_contract import CandleSchemaV1, expected_interval_ms

__all__ = [
    "CandleSchemaV1",
    "ColumnMapping",
    "ColumnMappingError",
    "DuckDBSettings",
    "FilenameParseError",
    "IngestOptions",
    "build_candle_inventory",
    "build_manifest_index",
    "create_duckdb_connection",
    "default_inventory_window",
    "detect_column_mapping",
    "estimate_recent_value_by_market",
    "expected_interval_ms",
    "ingest_dataset",
    "load_manifest",
    "manifest_path",
    "parse_utc_ts_ms",
    "parse_upbit_filename",
    "should_skip_file",
    "sniff_csv_files",
    "validate_dataset",
]
