# T23-14 Cross-Fit Density Ratio Weighting v1

## Goal

Reduce optimism in execution risk-control weighting by replacing in-sample
classifier diagnostics with out-of-fold density-ratio diagnostics.

## Design

- Keep the runtime density-ratio scorer as a single logistic model fitted on the
  full OOS decision row panel.
- Build stratified cross-fit folds across latest-window positives and historical
  negatives.
- Use out-of-fold probabilities to compute calibration-time importance weights,
  clip fraction, and diagnostic log loss.
- Persist only the final full-data model plus compact cross-fit diagnostics.

## Runtime Contract

- `weighting.density_ratio.mode` can now be
  `latest_window_logistic_density_ratio_crossfit_v1`.
- `classifier_status=ready_crossfit` signals that OOF weighting was used during
  contract construction.
- `crossfit_fold_count`, `crossfit_probability_source`, and
  `crossfit_log_loss` are exported for observability.

## Notes

- Runtime scoring still uses the full-data model.
- Cross-fit weights are used only during offline contract construction, which is
  the place where optimism bias mattered.
