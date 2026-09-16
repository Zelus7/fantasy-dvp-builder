# Waiver decision support v3

Implementation requested 2026-09-15. Release details and acceptance checks are in `waiver-v3-release.md`.

- [x] Refresh propagation, explicit readiness reasons, score explanations.
- [x] Injury return scenarios, legal weekly lineup comparisons, hold versus replace.
- [x] This week / four-week bridge / rest-of-season plans with alternatives and deadlines.
- [x] Current-season opportunity evidence, missing-data flags, source timestamps.
- [x] Verified league rules and budget, conservative bid ranges, no invented market odds.
- [x] Immutable decision inputs, archived calculation versions, outcome scoring, ESPN/hold baselines and manual claim feedback.
- [x] Regression tests including Brown/Pierce; isolated phone-width and real-roster snapshot browser checks.
- [x] Safe migration, deployment, successful production source refresh, release documentation and user checklist.
- [ ] User acceptance in the existing signed-in live browser; automation was blocked by its URL policy.
- [ ] Prospective fantasy performance validation after future weeks finish; no validated predictive edge is claimed yet.

Boundaries: no ESPN transactions, no credential rotation, no paid feed, no medical return predictions. Unknown data remains unknown. Future scenario totals are planning comparisons, not calibrated forecasts. Existing automated tests do not establish fantasy performance.
