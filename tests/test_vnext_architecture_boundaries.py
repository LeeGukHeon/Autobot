from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VNEXT_ROOTS = (
    ROOT / "autobot" / "app",
    ROOT / "autobot" / "domain",
    ROOT / "autobot" / "data" / "contracts",
    ROOT / "autobot" / "data" / "registry",
    ROOT / "autobot" / "data" / "derived",
    ROOT / "autobot" / "runtime",
    ROOT / "autobot" / "infra",
)


def test_vnext_modules_do_not_import_powershell_scripts_or_legacy_wrappers() -> None:
    forbidden_snippets = (
        "scripts.candidate_acceptance",
        "scripts.daily_champion_challenger_v4_for_server",
        "scripts.daily_champion_challenger_v5_for_server",
        "scripts.close_v5_train_ready_snapshot",
        "scripts.refresh_data_platform_layers",
    )

    for root in VNEXT_ROOTS:
        for path in root.rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            for snippet in forbidden_snippets:
                assert snippet not in source, f"{path} must not import legacy wrapper path {snippet}"
