# Opportunity forecast v1

Scope: learned offensive-player points and usage forecasts, waiver-focused historical evaluation, tested uncertainty, current-data integration, immutable replay, and live browser acceptance. No ESPN transactions or new paid data sources.

Predeclared protocol (before new holdout evaluation):
- Seed history 2018; fit on 2019–2022, select ridge penalty on 2023, refit through 2023, calibrate residual intervals on 2024, report 2025 once without tuning to it. The project previously inspected 2025 for older baselines; it is not a wholly unseen project-level season.
- Every input uses strictly earlier regular-season weeks. No current identity/team/depth metadata in historical features. No random row train/test split.
- Predict points, targets and carries for the next week and mean points per recorded appearance in the next four calendar weeks. Missing appearances are not fabricated zeroes. These are conditional-on-playing statistical forecasts, not injury/availability forecasts.
- Lower-ranked historical pools are proxies, not reconstructed ESPN waiver availability. Test add-versus-hold choices and regret as well as point error.
- Position/horizon promotion requires at least 200 holdout observations, at least 1% lower MAE, no worse RMSE, no worse proxy decision regret, and positive lower 95% weekly-block bootstrap bound for paired absolute-error improvement. ESPN projections remain primary until a genuine pre-deadline comparison supports replacing them.
- 80% residual intervals are calibrated on 2024; report measured 2025 coverage and sample sizes. Suppress probability claims unsupported by calibration.

Checklist:
- [x] Historical scoring aliases and leakage/target tests
- [x] Reproducible data/training/evaluation and compact versioned artifact
- [x] Waiver-focused metrics, uncertainty and honest limitations
- [x] Scheduled inference, bounded storage, freshness and fallback checks
- [x] Visible independent forecast, usage, range and promotion status
- [x] Frozen individual forecast versus ESPN accuracy tracking
- [ ] Regression suite, deployment, current data refresh, signed-in browser acceptance
- [ ] Merge and user checklist
