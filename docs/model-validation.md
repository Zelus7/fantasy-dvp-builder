# Decision model validation

Reviewed at Extra High on 2026-09-15. This is decision support, not a guarantee.

Historical note: the benchmark below predates the September 19 correction for
nflverse's renamed passing-interception column. It is retained as evidence of
the earlier selection, not as the current model scorecard. See
[Independent opportunity forecasts](forecast-v1.md) for the current chronological
tests, scoring correction, promotion gates and limitations. The waiver/trade
description below records the earlier v2 screen; current waiver behavior is
documented in [Waiver planning v3](waiver-v3-release.md).

## Historical fallback benchmark

Reproduce with `python -m pipeline.backtest --config-file <local config>`.
The config contains the connected league's scoring rules, not ESPN cookies.

Model selection used 2024 only. A separate 2025 holdout used strictly earlier
weeks and the prior regular season for each prediction. No target-week outcomes
entered the feature or defense constructors. The lineup-relevant population
was the same for every model: up to 12 QB/TE and 24 RB/WR per week, selected by
past baseline, with at least three historical stat lines and a target-week stat
line. This excludes DNP prediction and unseen rookies.

| Historical fallback | 2024 MAE (1,211 observations) | 2025 MAE (1,201 observations) |
| --- | ---: | ---: |
| Season baseline | 5.818 | 5.936 |
| 55% recent / 45% season | 5.972 | 6.134 |
| Recent blend + matchup/role/trend adjustments | 6.178 | 6.351 |

Lower is better. Season baseline was selected on development data and retained
on holdout. The application therefore uses the historical season baseline when
an exact-week ESPN projection is missing, with no extra unvalidated matchup,
role, trend or weather multiplier. ESPN projections remain unchanged when
present. DvP, role and weather remain contextual evidence and warnings.

These are custom-league fantasy-point errors. Unsupported scoring rules remain
approximate; ESPN actual scores are authoritative. This benchmark does not
evaluate ESPN projection accuracy, injury timing, real transactions, trade
acceptance, or future performance. Low/high scenarios are heuristic ranges,
not calibrated confidence intervals.

## Waivers and trades

The optimizer was checked against exhaustive legal assignment on 200 seeded
small rosters, including zero/negative estimates and flex eligibility. It first
fills legally fillable slots, then maximizes the configured score. Game locks,
hard injuries, missing estimates and explicit ESPN eligibility are respected.

Waiver Edge is optimized starter gain plus 15% of aggregate bench-baseline
change after a legal named add/drop. The weight is a transparent screening
rule, not a fitted expected-points model. Weekly moves must not sacrifice more
than 0.5 typical-week starter points. Protected players are never auto-dropped.

Trade discovery checks selected 1-for-1 and 2-for-2 packages from offensive
players. Each side must improve its legal typical-week starting lineup by at
least 0.5 points, with no coverage loss and no more than 20% exchanged baseline
imbalance. These are conservative screening rules, not market consensus or
acceptance odds. Unequal packages require verified extra roster actions and are
not automatically recommended. No ESPN transaction is executed.

ROS estimates are typical-week baselines, not total remaining-season forecasts.
ESPN season projections are preferred; weekly projections are a fallback.
Production and schedule weights remain explicit heuristics until prospective
snapshots support a true out-of-sample ROS benchmark.

## Sources and limitations

NFL team colors/logos: nflverse `load_teams` metadata, ESPN image URLs.
Historical statistics/schedules: nflverse through nflreadpy. Feed coverage is
reported by the actual regular-season week found, independently of fetch time.
NFL news: source-linked ESPN headlines with publication timestamps. This is not
a complete practice-report or confirmed-inactive feed. Roster injury labels
come from the connected ESPN league. Weather flags use Open-Meteo and venue
roof defaults; retractable-roof decisions require verification.
