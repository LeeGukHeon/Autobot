# Upbit AutoBot

Automated trading system scaffold for Upbit that keeps one consistent architecture across backtest, paper trading, and live execution.

## Scope (T00)
- Repository bootstrap and directory conventions
- Config-first skeleton (`config/*.yaml`)
- Python package + CLI entry point (`python -m autobot.cli --help`)
- Initial architecture docs and change policy

## Quickstart
```powershell
cd D:\MyApps\Autobot
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -r python\requirements.txt
python -m autobot.cli --help
```

## Structure
- `docs/`: architecture records, roadmap, ticket-level notes
- `config/`: environment and module-level configuration
- `python/autobot/`: core package for data, strategy, risk, execution, upbit client, backtest
- `cpp/`: optional high-performance components via pybind11

## Notes
- Runtime artifacts in `data/`, `models/`, `logs/` are local-only and gitignored.
- Fees, minimum order amount, and tick sizes must be fetched from Upbit APIs (no hardcoding).
