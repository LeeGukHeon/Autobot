# T21.1 Exact CTREND Factor Contract v1

## Goal
- Add a literature-grounded compact trend factor pack that is implemented as a formal contract, not as another ad hoc trend heuristic.
- Keep the current runtime contract unchanged.

## Reference
- Fieberg et al., "A Trend Factor for the Cross Section of Cryptocurrency Returns"
  - https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/trend-factor-for-the-cross-section-of-cryptocurrency-returns/4C1509ACBA33D5DCAF0AC24379148178

## Why This Fits The Current Server
- uses existing candle data
- no new raw archive family required
- low incremental disk cost
- high methodology value per byte

## Scope
In scope:
- define a versioned `ctrend_v1` factor spec from the paper's published construction
- derive the required low-frequency inputs from existing stored candles
- broadcast the low-frequency factor state into the current `5m` training rows
- persist factor metadata:
  - source paper
  - horizon set
  - transform steps
  - rank convention
  - lookahead guard
- add factor validation tests against hand-built fixtures

Out of scope:
- inventing alternative trend formulas under the same name
- adding a second raw storage lane
- changing runtime interfaces

## Exact Implementation Standard
- Before coding, encode the paper's published signal definition into a factor-spec document or dataclass.
- Persist the exact transform chain:
  - base inputs
  - normalization
  - cross-sectional ranking
  - aggregation
- If a paper detail must be adapted for intraday deployment, persist both:
  - `paper_original_definition`
  - `deployment_adaptation`
- Adaptation must be explicit and reversible.

## Deliverables
- new feature contract block for `ctrend_v1`
- metadata artifact describing exact formula provenance
- fixture-based unit tests
- training artifact notes showing when the factor was active

## Acceptance
- the factor can be regenerated deterministically from stored candle inputs
- the artifact says exactly which published construction was implemented
- no hidden runtime-only heuristic is needed to use the factor
- storage increase remains negligible relative to current `v4` features

## Resource Fit
- CPU: low
- RAM: low
- Disk: low

## Follow-On Path
- later expand from `ctrend_v1` to a broader literature factor library without changing the feature contract style
