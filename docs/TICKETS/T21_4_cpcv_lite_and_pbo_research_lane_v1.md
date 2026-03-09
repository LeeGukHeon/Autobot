# T21.4 CPCV-Lite And PBO Research Lane v1

## Goal
- Strengthen out-of-sample validation with a budget-aware implementation of purged combinatorial testing.
- Keep the production lane simple while making research evidence less dependent on one walk-forward view.

## References
- Arian, Norouzi, Seco, "Backtest Overfitting in the Machine Learning Era"
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4686376
- Bailey, Lopez de Prado, "The Deflated Sharpe Ratio"
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551

## Why This Fits The Current Server
- a full CPCV expansion would be too expensive
- a summary-only bounded variant is still feasible
- statistical validation quality rises without adding heavy raw datasets

## Scope
In scope:
- add a `research_only` `cpcv_lite` mode
- purge and embargo every split explicitly
- limit the number of evaluated combinations to a bounded server-safe budget
- persist only summary outputs:
  - fold definitions
  - fold metrics
  - PBO
  - DSR
  - insufficiency reasons

Out of scope:
- making CPCV the default production gate immediately
- storing full duplicated fold artifacts
- broad hyperparameter explosions inside each fold

## Exact Implementation Standard
- split generation must be deterministic and reproducible
- every emitted report must list:
  - group count
  - embargo size
  - chosen combinations
  - skipped combinations
  - budget reason
- if the method is budget-cut, the report must explicitly say that the estimate is `lite`

## Deliverables
- `cpcv_lite` split generator
- summary-only fold ledger
- PBO and DSR report schema
- bounded tests on synthetic panels

## Acceptance
- research mode can run on the current box without exploding disk usage
- the system can say why a CPCV estimate is trusted, partial, or insufficient
- fold reports remain compact and reproducible

## Resource Fit
- CPU: medium
- RAM: medium
- Disk: low

## Follow-On Path
- if a stronger research machine appears later, increase combination count without changing the report contract
