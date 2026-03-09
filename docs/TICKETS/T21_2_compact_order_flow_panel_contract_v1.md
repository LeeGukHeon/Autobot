# T21.2 Compact Order-Flow Panel Contract v1

## Goal
- Build a formal order-flow predictor panel from already collected market data.
- Make the panel extension-ready for future multi-venue expansion without requiring that expansion now.

## Reference
- "Order Flow and Cryptocurrency Returns"
  - https://www.sciencedirect.com/science/article/pii/S1386418126000029

## Why This Fits The Current Server
- the server already stores `raw_ticks` and `raw_ws`
- the upgrade can be made mostly at the aggregated-feature level
- it increases signal quality without forcing a large raw-data explosion

## Scope
In scope:
- define a versioned order-flow panel contract
- compute compact flow predictors from existing venue-local data:
  - signed volume imbalance
  - normalized flow imbalance
  - persistence across bounded horizons
  - spread/depth-conditioned flow state
- persist only compact aggregated feature tables and fit diagnostics
- keep field names and schema extensible to later multi-venue joins

Out of scope:
- full global multi-venue raw ingestion
- deep order-book replay archives
- GPU-heavy sequence modeling

## Exact Implementation Standard
- panel fields must correspond to explicit mathematical definitions
- scaling choices must be persisted:
  - denominator
  - horizon
  - winsorization or clipping rule
  - missing-data handling
- if only one venue is available, the contract must still keep:
  - `venue_id`
  - `aggregation_scope`
  so the same schema can later hold multi-venue data

## Deliverables
- `order_flow_panel_v1` schema
- compact feature builder from existing trade/order-book snapshots
- summary diagnostics:
  - coverage
  - staleness
  - per-horizon availability
- regression tests on deterministic toy data

## Acceptance
- panel rows are reproducible from stored inputs
- every field has a stable definition and version
- no hidden heuristic remapping is required later to extend the panel to more venues
- disk growth stays bounded to compact parquet summaries

## Resource Fit
- CPU: low to medium
- RAM: low
- Disk: low to medium

## Follow-On Path
- phase 2 can add multi-venue joins and lead-lag aggregation without changing the field semantics
