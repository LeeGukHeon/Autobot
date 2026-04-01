from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_adopt_v5_candidate_wrapper_uses_v5_defaults() -> None:
    source = (REPO_ROOT / "scripts" / "adopt_v5_candidate_for_server.ps1").read_text(encoding="utf-8")
    assert 'logs/model_v5_candidate' in source
    assert 'autobot-paper-v5.service' in source
    assert 'autobot-paper-v5-paired.service' in source
    assert 'autobot-live-alpha-canary.service' in source


def test_daily_champion_challenger_v5_wrapper_uses_v5_defaults() -> None:
    source = (REPO_ROOT / "scripts" / "daily_champion_challenger_v5_for_server.ps1").read_text(encoding="utf-8")
    assert 'logs/model_v5_candidate' in source
    assert 'autobot-paper-v5.service' in source
    assert '[string]$ChallengerUnitName = ""' in source
    assert 'autobot-paper-v5-paired.service' in source
    assert 'autobot-live-alpha-canary.service' in source
    assert 'autobot-v5-challenger-spawn.timer' in source
    assert 'autobot-v5-challenger-promote.timer' in source
    assert 'run_candles_api_refresh.ps1' in source
    assert 'run_raw_ticks_daily.ps1' in source
    assert 'close_v5_train_ready_snapshot.ps1' in source
    assert '-SkipDeadline' in source
    assert '-SkipDailyPipeline:$true' in source


def test_install_server_daily_v5_split_wrapper_targets_v5_units() -> None:
    source = (REPO_ROOT / "scripts" / "install_server_daily_v5_split_challenger_services.ps1").read_text(encoding="utf-8")
    assert 'daily_champion_challenger_v5_for_server.ps1' in source
    assert 'adopt_v5_candidate_for_server.ps1' in source
    assert 'autobot-v5-challenger-spawn.service' in source
    assert 'autobot-v5-challenger-promote.service' in source
    assert 'autobot-raw-ticks-daily.timer' in source
    assert 'autobot-v5-train-snapshot-close.timer' in source
    assert '/tmp/autobot-v5-nightly-train-chain.lock' in source
