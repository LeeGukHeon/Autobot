"""Model card generator."""

from __future__ import annotations

from typing import Any


def render_model_card(
    *,
    run_id: str,
    model_family: str,
    champion: str,
    metrics: dict[str, Any],
    thresholds: dict[str, Any],
    data_fingerprint: dict[str, Any],
) -> str:
    rows = metrics.get("rows", {}) if isinstance(metrics, dict) else {}
    champion_metrics = metrics.get("champion_metrics", {}) if isinstance(metrics, dict) else {}
    cls = champion_metrics.get("classification", {}) if isinstance(champion_metrics, dict) else {}
    trade = champion_metrics.get("trading", {}) if isinstance(champion_metrics, dict) else {}
    top5 = trade.get("top_5pct", {}) if isinstance(trade, dict) else {}

    lines = [
        f"# Model Card: {run_id}",
        "",
        "## Summary",
        f"- model_family: `{model_family}`",
        f"- champion_track: `{champion}`",
        f"- rows(train/valid/test/drop): {rows.get('train', 0)}/{rows.get('valid', 0)}/{rows.get('test', 0)}/{rows.get('drop', 0)}",
        "",
        "## Champion Metrics",
        f"- ROC-AUC: {_fmt(cls.get('roc_auc'))}",
        f"- PR-AUC: {_fmt(cls.get('pr_auc'))}",
        f"- LogLoss: {_fmt(cls.get('log_loss'))}",
        f"- Brier: {_fmt(cls.get('brier_score'))}",
        f"- Precision@Top5%: {_fmt(top5.get('precision'))}",
        f"- EV_net@Top5%: {_fmt(top5.get('ev_net'))}",
        "",
        "## Thresholds",
    ]

    for key in sorted(thresholds.keys()):
        value = thresholds.get(key)
        lines.append(f"- {key}: {_fmt(value)}")

    lines.extend(
        [
            "",
            "## Data Fingerprint",
            f"- dataset_root: `{data_fingerprint.get('dataset_root', '')}`",
            f"- tf: `{data_fingerprint.get('tf', '')}`",
            f"- quote/top_n: `{data_fingerprint.get('quote', '')}` / `{data_fingerprint.get('top_n', '')}`",
            f"- start_ts_ms/end_ts_ms: `{data_fingerprint.get('start_ts_ms', '')}` / `{data_fingerprint.get('end_ts_ms', '')}`",
            f"- manifest_sha256: `{data_fingerprint.get('manifest_sha256', '')}`",
            f"- feature_spec_sha256: `{data_fingerprint.get('feature_spec_sha256', '')}`",
            f"- label_spec_sha256: `{data_fingerprint.get('label_spec_sha256', '')}`",
            "",
        ]
    )
    return "\n".join(lines)


def _fmt(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return f"{float(value):.6f}"
    return str(value)
