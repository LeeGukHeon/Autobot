from __future__ import annotations

import polars as pl

import json

from autobot.data.contracts.completeness import (
    CandleCoverageRequest,
    DatasetCoverageRequest,
    SequenceDateCompletenessRequest,
    summarize_candle_coverage,
    summarize_dataset_coverage,
    summarize_sequence_date_completeness,
)
from autobot.data.sources.completeness import (
    LobCoverageRequest,
    MicroCoverageRequest,
    SequenceCoverageRequest,
    summarize_lob_coverage,
    summarize_micro_coverage,
    summarize_sequence_coverage,
)
from autobot.domain.universe.tradeable import resolve_tradeable_markets
from autobot.runtime.feature_plane.runtime_universe import (
    build_runtime_universe_snapshot,
    determine_missing_live_inputs,
    intersect_runtime_markets,
)
from autobot.runtime.scanner.top_markets import TopTradeValueScanner, scan_top_markets
from autobot.strategy.micro_snapshot import MicroSnapshot
from autobot.upbit.ws.models import TickerEvent


def test_vnext_candle_coverage_contract_reports_missing_and_stale_pairs(tmp_path) -> None:
    manifest_path = tmp_path / "manifest.parquet"
    pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-BTC", "KRW-ETH"],
            "tf": ["1m", "60m", "1m"],
            "min_ts_ms": [1_000, 1_000, 1_000],
            "max_ts_ms": [5_000, 3_000, 5_000],
            "rows": [10, 2, 10],
        }
    ).write_parquet(manifest_path)

    result = summarize_candle_coverage(
        CandleCoverageRequest(
            manifest_path=manifest_path,
            markets=("KRW-BTC", "KRW-ETH"),
            required_tfs=("1m", "60m"),
            required_end_ts_ms=4_000,
        )
    )

    assert result.pass_ is False
    assert ("KRW-BTC", "60m") in result.stale_pairs
    assert ("KRW-ETH", "60m") in result.missing_pairs


def test_vnext_tradeable_universe_preserves_live_order() -> None:
    result = resolve_tradeable_markets(
        live_scan_markets=("KRW-ETH", "KRW-BTC", "KRW-XRP"),
        runtime_allowed_markets=("KRW-BTC", "KRW-XRP"),
    )

    assert result.markets == ("KRW-BTC", "KRW-XRP")
    assert result.live_scan_markets == ("KRW-ETH", "KRW-BTC", "KRW-XRP")


def test_vnext_dataset_coverage_contract_reports_missing_and_stale_markets(tmp_path) -> None:
    manifest_path = tmp_path / "lob_manifest.parquet"
    pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-ETH"],
            "date": ["2026-04-12", "2026-04-12"],
            "max_ts_ms": [5_000, 3_000],
            "rows": [10, 10],
        }
    ).write_parquet(manifest_path)

    result = summarize_dataset_coverage(
        DatasetCoverageRequest(
            manifest_path=manifest_path,
            markets=("KRW-BTC", "KRW-ETH", "KRW-XRP"),
            required_end_ts_ms=4_000,
        )
    )

    assert result.pass_ is False
    assert result.stale_markets == ("KRW-ETH",)
    assert result.missing_markets == ("KRW-XRP",)


def test_vnext_sequence_date_completeness_contract_uses_cached_date_status(tmp_path) -> None:
    path = tmp_path / "date_completeness.json"
    path.write_text(
        json.dumps(
            {
                "policy": "sequence_tensor_date_completeness_v1",
                "dates": {
                    "2026-04-12": {
                        "date": "2026-04-12",
                        "selected_markets": ["KRW-BTC", "KRW-ETH"],
                        "complete": True,
                        "reused_anchor_count": 128,
                        "max_anchors_per_market": 64,
                        "date_validity_signature": "sig-1",
                        "per_market": [
                            {"market": "KRW-BTC", "reusable_count": 64},
                            {"market": "KRW-ETH", "reusable_count": 64},
                        ],
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = summarize_sequence_date_completeness(
        SequenceDateCompletenessRequest(
            date_completeness_path=path,
            date_value="2026-04-12",
            markets=("KRW-BTC", "KRW-ETH"),
            max_anchors_per_market=64,
            required_validity_signature="sig-1",
        )
    )

    assert result.pass_ is True
    assert result.reused_anchor_count == 128
    assert result.incomplete_markets == ()


def test_vnext_micro_source_completeness_bridge_uses_micro_manifest(tmp_path) -> None:
    out_root = tmp_path / "micro_v1"
    meta_root = out_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "run_id": ["run-1", "run-1"],
            "tf": ["1m", "1m"],
            "market": ["KRW-BTC", "KRW-ETH"],
            "date": ["2026-04-12", "2026-04-12"],
            "rows": [10, 10],
            "min_ts_ms": [1_000, 1_000],
            "max_ts_ms": [5_000, 3_000],
            "micro_available_rows": [10, 10],
            "micro_trade_available_rows": [10, 10],
            "micro_book_available_rows": [10, 10],
            "trade_source_ws_rows": [10, 10],
            "trade_source_rest_rows": [0, 0],
            "trade_source_none_rows": [0, 0],
            "part_file": ["a", "b"],
            "status": ["OK", "OK"],
            "reasons_json": ["[]", "[]"],
            "error_message": [None, None],
            "built_at_ms": [1, 1],
        }
    ).write_parquet(meta_root / "manifest.parquet")

    result = summarize_micro_coverage(
        MicroCoverageRequest(
            out_root=out_root,
            markets=("KRW-BTC", "KRW-ETH"),
            required_end_ts_ms=4_000,
            tf="1m",
        )
    )

    assert result.pass_ is False
    assert result.stale_markets == ("KRW-ETH",)


def test_vnext_lob_source_completeness_bridge_uses_lob_manifest(tmp_path) -> None:
    dataset_root = tmp_path / "lob30_v1"
    meta_root = dataset_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "dataset_name": ["lob30_v1"],
            "source": ["upbit_ws_orderbook_30"],
            "window_tag": ["run"],
            "market": ["KRW-BTC"],
            "date": ["2026-04-12"],
            "rows": [10],
            "min_ts_ms": [1_000],
            "max_ts_ms": [5_000],
            "status": ["OK"],
            "reasons_json": ["[]"],
            "error_message": [None],
            "part_file": ["a"],
            "collected_at": [1],
        }
    ).write_parquet(meta_root / "manifest.parquet")

    result = summarize_lob_coverage(
        LobCoverageRequest(
            dataset_root=dataset_root,
            markets=("KRW-BTC", "KRW-ETH"),
            required_end_ts_ms=4_000,
        )
    )

    assert result.pass_ is False
    assert result.missing_markets == ("KRW-ETH",)


def test_vnext_sequence_source_completeness_bridge_uses_date_completeness(tmp_path) -> None:
    out_root = tmp_path / "sequence_v1"
    meta_root = out_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    (meta_root / "date_completeness.json").write_text(
        json.dumps(
            {
                "policy": "sequence_tensor_date_completeness_v1",
                "dates": {
                    "2026-04-12": {
                        "date": "2026-04-12",
                        "selected_markets": ["KRW-BTC"],
                        "complete": True,
                        "reused_anchor_count": 64,
                        "max_anchors_per_market": 64,
                        "date_validity_signature": "sig-1",
                        "per_market": [
                            {"market": "KRW-BTC", "reusable_count": 64},
                        ],
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = summarize_sequence_coverage(
        SequenceCoverageRequest(
            out_root=out_root,
            date_value="2026-04-12",
            markets=("KRW-BTC",),
            max_anchors_per_market=64,
            required_validity_signature="sig-1",
        )
    )

    assert result.pass_ is True
    assert result.reused_anchor_count == 64


def test_vnext_runtime_market_intersection_preserves_live_scan_order() -> None:
    assert intersect_runtime_markets(
        live_markets=("KRW-ADA", "KRW-BTC", "KRW-ETH"),
        runtime_allowed_markets=("KRW-ETH", "KRW-BTC"),
    ) == ("KRW-BTC", "KRW-ETH")


def test_vnext_runtime_universe_requires_ticker_trade_and_orderbook() -> None:
    class _MicroProvider:
        def get(self, market: str, ts_ms: int):  # noqa: ANN201
            if market == "KRW-BTC":
                return MicroSnapshot(
                    market=market,
                    snapshot_ts_ms=ts_ms,
                    last_event_ts_ms=ts_ms,
                    trade_events=5,
                    trade_coverage_ms=1_000,
                    book_events=4,
                    book_coverage_ms=1_000,
                    book_available=True,
                )
            return MicroSnapshot(
                market=market,
                snapshot_ts_ms=ts_ms,
                last_event_ts_ms=ts_ms,
                trade_events=5,
                trade_coverage_ms=1_000,
                book_events=0,
                book_coverage_ms=0,
                book_available=False,
            )

    items = [
        type("Item", (), {"market": "KRW-BTC", "ts_ms": 1_000, "trade_price": 100.0})(),
        type("Item", (), {"market": "KRW-ETH", "ts_ms": 1_000, "trade_price": 100.0})(),
    ]

    missing = determine_missing_live_inputs(
        market_items=items,
        micro_snapshot_provider=_MicroProvider(),
    )

    assert missing["KRW-BTC"] == ()
    assert missing["KRW-ETH"] == ("orderbook",)


def test_vnext_runtime_universe_snapshot_keeps_nontradeable_markets_visible(tmp_path) -> None:
    class _PublicClient:
        def markets(self, *, is_details: bool = True):  # noqa: ANN201
            _ = is_details
            return [{"market": "KRW-BTC"}, {"market": "KRW-ETH"}]

    class _MicroProvider:
        def get(self, market: str, ts_ms: int):  # noqa: ANN201
            if market == "KRW-BTC":
                return MicroSnapshot(
                    market=market,
                    snapshot_ts_ms=ts_ms,
                    last_event_ts_ms=ts_ms,
                    trade_events=1,
                    trade_coverage_ms=1_000,
                    book_events=1,
                    book_coverage_ms=1_000,
                    book_available=True,
                )
            return None

    items = [
        type("Item", (), {"market": "KRW-BTC", "ts_ms": 1_000, "trade_price": 100.0})(),
        type("Item", (), {"market": "KRW-ETH", "ts_ms": 1_000, "trade_price": 100.0})(),
    ]

    snapshot = build_runtime_universe_snapshot(
        public_client=_PublicClient(),
        quote="KRW",
        allowed_markets=("KRW-BTC", "KRW-ETH"),
        market_items=items,
        micro_snapshot_provider=_MicroProvider(),
    )

    assert snapshot.live_markets == ("KRW-BTC", "KRW-ETH")
    assert snapshot.tradeable_markets == ("KRW-BTC",)
    assert snapshot.missing_live_inputs_by_market["KRW-ETH"] == ("trade", "orderbook")


def test_vnext_top_market_scanner_bridge_uses_existing_scanner() -> None:
    scanner = TopTradeValueScanner()
    scanner.update(
        TickerEvent(
            market="KRW-BTC",
            ts_ms=1_000,
            trade_price=100.0,
            acc_trade_price_24h=1_000.0,
        )
    )
    scanner.update(
        TickerEvent(
            market="KRW-ETH",
            ts_ms=1_000,
            trade_price=100.0,
            acc_trade_price_24h=2_000.0,
        )
    )

    top = scan_top_markets(scanner, n=1, quote="KRW")

    assert [item.market for item in top] == ["KRW-ETH"]


def test_vnext_package_layout_is_importable() -> None:
    import autobot.app.bootstrap as app_bootstrap
    import autobot.data.derived.features.v4 as features_v4
    import autobot.data.derived.labels.private_execution as private_labels
    import autobot.data.derived.tensors.sequence as sequence_tensors
    import autobot.runtime.feature_plane.online_v5 as online_v5
    import autobot.runtime.selector.candidate_builder as candidate_builder

    assert callable(app_bootstrap.build_default_container)
    assert hasattr(features_v4, "build_v4_features_dataset")
    assert hasattr(private_labels, "build_private_execution_labels")
    assert hasattr(sequence_tensors, "build_sequence_tensors")
    assert hasattr(online_v5, "LiveFeatureProviderV5")
    assert hasattr(candidate_builder, "CandidateGeneratorV1")
