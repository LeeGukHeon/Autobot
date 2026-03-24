# Upbit AutoBot

Automated trading system scaffold for Upbit that keeps one consistent architecture across backtest, paper trading, and live execution.

## Mandatory Start For Implementation Contexts

If you are starting a new implementation context for this repository, do not begin from memory or ad-hoc exploration.

Start here, in this order:

1. [START_HERE_NEXT_CONTEXT.md](/d:/MyApps/Autobot/START_HERE_NEXT_CONTEXT.md)
2. [CODEX_MANDATORY_WORK_PRINCIPLES_2026-03-25.md](/d:/MyApps/Autobot/docs/CODEX_MANDATORY_WORK_PRINCIPLES_2026-03-25.md)
3. [NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md](/d:/MyApps/Autobot/docs/NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md)
4. [INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md](/d:/MyApps/Autobot/docs/INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md)

The next implementation session is expected to follow the checklist in the mandatory execution protocol and work from the first unchecked item without skipping ahead.

Unless the user explicitly waives it, implementation work in this repository is not considered complete until it has been committed, pushed, pulled on the OCI server, and then validated there.

OCI server access reference:
- [OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md](/d:/MyApps/Autobot/docs/OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md)

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
- `docs/`: architecture records, roadmap, unified runbook, ticket-level notes
- `config/`: environment and module-level configuration
- `autobot/`: core package (SSOT) for data, strategy, risk, execution, upbit client, backtest
- `python/`: requirements/tooling directory, optional mirror target (`python/autobot`)
- `cpp/`: optional high-performance components via pybind11

## Notes
- Runtime artifacts in `data/`, `models/`, `logs/` are local-only and gitignored.
- Fees, minimum order amount, and tick sizes must be fetched from Upbit APIs (no hardcoding).
- Current program-wide overview is maintained in `docs/PROGRAM_RUNBOOK.md`.
