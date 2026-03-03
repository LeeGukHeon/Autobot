"""Configuration and spec helpers for feature store v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

from autobot.data import expected_interval_ms


DEFAULT_FEATURES_YAML = "features.yaml"
VALID_FLOAT_DTYPES = {"float32", "float64"}
VALID_UNIVERSE_MODES = {"static_start", "fixed_list"}
VALID_NEUTRAL_POLICIES = {"drop", "keep_as_class"}


@dataclass(frozen=True)
class FeatureWindows:
    ret: tuple[int, ...] = (1, 3, 6, 12)
    rv: tuple[int, ...] = (12, 36)
    ema: tuple[int, ...] = (12, 36)
    rsi: int = 14
    atr: int = 14
    vol_z: int = 36


@dataclass(frozen=True)
class FeatureSetV1Config:
    windows: FeatureWindows = FeatureWindows()
    enable_factor_features: bool = True
    factor_markets: tuple[str, ...] = ("KRW-BTC", "KRW-ETH")
    enable_liquidity_rank: bool = False


@dataclass(frozen=True)
class LabelV1Config:
    horizon_bars: int = 12
    thr_bps: float = 15.0
    neutral_policy: str = "drop"
    fee_bps_est: float = 10.0
    safety_bps: float = 5.0


@dataclass(frozen=True)
class UniverseConfig:
    quote: str = "KRW"
    mode: str = "static_start"
    top_n: int = 20
    lookback_days: int = 7
    fixed_list: tuple[str, ...] = ()


@dataclass(frozen=True)
class TimeRangeConfig:
    start: str = "2024-01-01"
    end: str = "2026-03-01"


@dataclass(frozen=True)
class FeatureBuildConfig:
    dataset_name: str = "features_v1"
    input_dataset: str = "candles_v1"
    float_dtype: str = "float32"


@dataclass(frozen=True)
class FeaturesConfig:
    build: FeatureBuildConfig
    parquet_root: Path
    features_root: Path
    universe: UniverseConfig
    time_range: TimeRangeConfig
    feature_set_v1: FeatureSetV1Config
    label_v1: LabelV1Config

    @property
    def dataset_name(self) -> str:
        return self.build.dataset_name

    @property
    def input_dataset(self) -> str:
        return self.build.input_dataset

    @property
    def float_dtype(self) -> str:
        return self.build.float_dtype

    @property
    def input_dataset_root(self) -> Path:
        return self.parquet_root / self.input_dataset

    @property
    def output_dataset_root(self) -> Path:
        return self.features_root / self.dataset_name


def load_features_config(
    config_dir: Path,
    *,
    base_config: dict[str, Any] | None = None,
    filename: str = DEFAULT_FEATURES_YAML,
) -> FeaturesConfig:
    base = base_config if isinstance(base_config, dict) else {}
    raw = _load_yaml_doc(config_dir / filename)

    storage_cfg = base.get("storage", {}) if isinstance(base.get("storage"), dict) else {}
    data_cfg = base.get("data", {}) if isinstance(base.get("data"), dict) else {}
    features_cfg = raw.get("features", {}) if isinstance(raw.get("features"), dict) else {}
    universe_cfg = raw.get("universe", {}) if isinstance(raw.get("universe"), dict) else {}
    time_range_cfg = raw.get("time_range", {}) if isinstance(raw.get("time_range"), dict) else {}
    set_cfg = raw.get("feature_set_v1", {}) if isinstance(raw.get("feature_set_v1"), dict) else {}
    label_cfg = raw.get("label_v1", {}) if isinstance(raw.get("label_v1"), dict) else {}

    dataset_name = str(features_cfg.get("dataset_name", "features_v1")).strip() or "features_v1"
    input_dataset = str(features_cfg.get("input_dataset", "candles_v1")).strip() or "candles_v1"

    float_dtype = str(features_cfg.get("float_dtype", "float32")).strip().lower() or "float32"
    if float_dtype not in VALID_FLOAT_DTYPES:
        raise ValueError(f"features.float_dtype must be one of: {', '.join(sorted(VALID_FLOAT_DTYPES))}")

    parquet_root = Path(
        str(
            features_cfg.get(
                "parquet_root",
                data_cfg.get("parquet_root", storage_cfg.get("parquet_dir", "data/parquet")),
            )
        )
    )
    features_root = Path(str(features_cfg.get("features_root", storage_cfg.get("features_dir", "data/features"))))

    quote = str(universe_cfg.get("quote", "KRW")).strip().upper() or "KRW"
    mode = str(universe_cfg.get("mode", "static_start")).strip().lower() or "static_start"
    if mode not in VALID_UNIVERSE_MODES:
        raise ValueError(f"universe.mode must be one of: {', '.join(sorted(VALID_UNIVERSE_MODES))}")
    top_n = max(1, int(universe_cfg.get("top_n", 20)))
    lookback_days = max(1, int(universe_cfg.get("lookback_days", 7)))
    fixed_list = tuple(_normalize_market(item) for item in universe_cfg.get("fixed_list", []) if str(item).strip())

    start = str(time_range_cfg.get("start", "2024-01-01")).strip() or "2024-01-01"
    end = str(time_range_cfg.get("end", "2026-03-01")).strip() or "2026-03-01"
    _ = parse_date_to_ts_ms(start)
    _ = parse_date_to_ts_ms(end, end_of_day=True)

    windows_cfg = set_cfg.get("windows", {}) if isinstance(set_cfg.get("windows"), dict) else {}
    ret_windows = _coerce_positive_int_tuple(windows_cfg.get("ret", (1, 3, 6, 12)))
    rv_windows = _coerce_positive_int_tuple(windows_cfg.get("rv", (12, 36)))
    ema_windows = _coerce_positive_int_tuple(windows_cfg.get("ema", (12, 36)))
    if len(ema_windows) < 2:
        raise ValueError("feature_set_v1.windows.ema must contain at least two windows")
    if 1 not in ret_windows:
        raise ValueError("feature_set_v1.windows.ret must include 1 for rv/factor features")
    windows = FeatureWindows(
        ret=ret_windows,
        rv=rv_windows,
        ema=ema_windows,
        rsi=max(1, int(windows_cfg.get("rsi", 14))),
        atr=max(1, int(windows_cfg.get("atr", 14))),
        vol_z=max(1, int(windows_cfg.get("vol_z", 36))),
    )

    factor_markets = tuple(
        _normalize_market(item)
        for item in set_cfg.get("factor_markets", ("KRW-BTC", "KRW-ETH"))
        if str(item).strip()
    )
    if not factor_markets:
        factor_markets = ("KRW-BTC", "KRW-ETH")

    feature_set_v1 = FeatureSetV1Config(
        windows=windows,
        enable_factor_features=bool(set_cfg.get("enable_factor_features", True)),
        factor_markets=factor_markets,
        enable_liquidity_rank=bool(set_cfg.get("enable_liquidity_rank", False)),
    )

    neutral_policy = str(label_cfg.get("neutral_policy", "drop")).strip().lower() or "drop"
    if neutral_policy not in VALID_NEUTRAL_POLICIES:
        raise ValueError(f"label_v1.neutral_policy must be one of: {', '.join(sorted(VALID_NEUTRAL_POLICIES))}")

    label_v1 = LabelV1Config(
        horizon_bars=max(1, int(label_cfg.get("horizon_bars", 12))),
        thr_bps=float(label_cfg.get("thr_bps", 15.0)),
        neutral_policy=neutral_policy,
        fee_bps_est=float(label_cfg.get("fee_bps_est", 10.0)),
        safety_bps=float(label_cfg.get("safety_bps", 5.0)),
    )

    return FeaturesConfig(
        build=FeatureBuildConfig(
            dataset_name=dataset_name,
            input_dataset=input_dataset,
            float_dtype=float_dtype,
        ),
        parquet_root=parquet_root,
        features_root=features_root,
        universe=UniverseConfig(
            quote=quote,
            mode=mode,
            top_n=top_n,
            lookback_days=lookback_days,
            fixed_list=fixed_list,
        ),
        time_range=TimeRangeConfig(start=start, end=end),
        feature_set_v1=feature_set_v1,
        label_v1=label_v1,
    )


def parse_date_to_ts_ms(value: str, *, end_of_day: bool = False) -> int:
    parsed = date.fromisoformat(str(value).strip())
    day_time = time(23, 59, 59, 999000) if end_of_day else time(0, 0, 0, 0)
    dt = datetime.combine(parsed, day_time, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def feature_columns(cfg: FeatureSetV1Config) -> list[str]:
    columns: list[str] = []
    for win in cfg.windows.ret:
        columns.append(f"log_ret_{win}")
    for win in cfg.windows.rv:
        columns.append(f"rv_{win}")
    for win in sorted(cfg.windows.ema):
        columns.append(f"ema_{win}")
    columns.append("ema_ratio")
    columns.append(f"rsi_{cfg.windows.rsi}")
    columns.append(f"atr_{cfg.windows.atr}")
    columns.append("hl_pct")
    columns.append("vol_log")
    columns.append(f"vol_z_{cfg.windows.vol_z}")
    columns.append("is_gap")
    columns.append("candle_ok")

    if cfg.enable_factor_features:
        rv_window = max(cfg.windows.rv)
        for market in cfg.factor_markets:
            prefix = factor_prefix(market)
            columns.append(f"{prefix}_log_ret_1")
            columns.append(f"{prefix}_rv_{rv_window}")

    if cfg.enable_liquidity_rank:
        columns.extend(["vol_quote_est", "vol_quote_24h_roll", "vol_rank_at_ts"])

    return _dedupe_preserve(columns)


def label_columns() -> list[str]:
    return ["y_reg", "y_cls"]


def factor_prefix(market: str) -> str:
    text = _normalize_market(market)
    if "-" in text:
        symbol = text.split("-", 1)[1]
    else:
        symbol = text
    safe = "".join(ch for ch in symbol.lower() if ch.isalnum() or ch == "_")
    return safe or "factor"


def max_feature_lookback_bars(cfg: FeatureSetV1Config, *, tf: str) -> int:
    max_window = max(
        max(cfg.windows.ret),
        max(cfg.windows.rv),
        max(cfg.windows.ema),
        cfg.windows.rsi,
        cfg.windows.atr,
        cfg.windows.vol_z,
    )
    if cfg.enable_liquidity_rank:
        bars_24h = max(1, int(86_400_000 / expected_interval_ms(tf)))
        max_window = max(max_window, bars_24h)
    return max_window


def effective_threshold_bps(cfg: LabelV1Config) -> float:
    if cfg.thr_bps > 0:
        return float(cfg.thr_bps)
    return float(cfg.fee_bps_est + cfg.safety_bps)


def to_serializable_config(cfg: FeaturesConfig) -> dict[str, Any]:
    payload = asdict(cfg)
    payload["parquet_root"] = str(cfg.parquet_root)
    payload["features_root"] = str(cfg.features_root)
    return payload


def sha256_json(payload: Any) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _coerce_positive_int_tuple(values: Any) -> tuple[int, ...]:
    if isinstance(values, (list, tuple)):
        result = tuple(max(1, int(item)) for item in values)
        if result:
            return result
    value = max(1, int(values))
    return (value,)


def _normalize_market(value: Any) -> str:
    return str(value).strip().upper()


def _dedupe_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _load_yaml_doc(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    return raw
