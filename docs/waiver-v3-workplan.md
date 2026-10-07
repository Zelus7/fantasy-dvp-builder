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

## Calendar planning and role research (2026-10-06)

- Trades now compare both teams' named weekly starters across one week, four weeks or the remaining season, with an explicit effective week, byes and early/planning/late injury returns. Manual 1-for-1, 2-for-2 and 2-for-1 analysis includes named extra drops and optional conditional free-agent additions. Empty roster spots have no assumed value. Missing schedule/estimate coverage withholds the full-window value claim.
- Discovery is bounded to eight baseline-ranked eligible players per team and four candidates per package shape per rival. The 20% exchanged-baseline balance and 0.5 starter-point/week mutual-gain rules are screening policies, not market prices or acceptance probabilities. A passing result is a discussion candidate, not an automatic offer.
- Waiver screening includes future-only bye value, near-term starter loss from drops, and positional depth. A team already carrying a spare QB receives QB-for-QB pickup plans rather than cross-position sacrifices. Kickers/defenses retain their same-position streaming guard.
- Waiver details identify immediate starter displacement, conditional future starting weeks, next review point and invalidating events. The separate emerging-role queue flags latest-week targets/carries/snaps increases, red-zone work and same-team injury openings even when a candidate fails the projection screen. It is research, not a claim recommendation, and never increases the point estimate. Weekly inside-20 and inside-10 usage preserve missing play-by-play as null.
- Validation history is week-selectable and cursor-paginated, defaults to the latest archived completed week, and replays one selected frozen decision on demand. Current-week refreshes no longer hide older decisions. Existing archives remain immutable; repeated snapshots are not independent trials.

Verification covers 202 application tests, 63 pipeline tests, Worker dry-run and secret scan, synthetic desktop/390px browser flows, plus the frozen September 30 pool of 419 free agents. That replay is a software regression check, not a new accuracy test or permission to retune to known outcomes. Learned coefficients and forecast promotion gates are unchanged. No ESPN transactions, schema changes, credentials or connector permissions are part of this release.

Acceptance: review a current waiver card's “Use now, reassess later”; inspect “Emerging roles & injury openings”; test a specific trade including its extra drop and weekly starters; select Week 4 under “Review validation history” and open an older frozen decision.
