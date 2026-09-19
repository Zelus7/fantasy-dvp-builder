# Independent opportunity forecasts

This release adds a trained statistical model, not new hand-set fantasy-point multipliers. It does not claim to beat ESPN.

## What is predicted

Separate models for QB, RB, WR and TE predict fantasy points, targets and carries from lagged production, volume, target/carry share, air-yard share, recent changes, sample size and gaps in observed play. The model is a standardized ridge regression; its coefficients and regularization are fitted from historical data.

Two targets are deliberately distinct:

- One week: points and workload conditional on a recorded appearance in that week.
- Four calendar weeks: mean points and workload per recorded appearance in the window, **not total fantasy points over four weeks**. Missing appearances and byes are not fabricated zeroes. This forecast is unavailable after Week 15.

Current injuries, late-breaking news, depth charts, snaps and red-zone evidence remain important context. They are not all fitted inputs in this version, and the model does not predict medical recovery or inactive status. New players with fewer than three historical stat lines do not get invented estimates. Changed teams, stale/wrong-week data, and long usage gaps prevent model promotion for the player.

## Reproducible historical protocol

`python -m pipeline.train_forecast --config-file <private league-scoring config>`

No credentials are needed for training. Public nflverse stats are cached under `.artifacts/forecast-v1/`; player-level evaluations remain local and ignored. Only compact model parameters and aggregate evaluation are committed. The model is fitted to this league's scoring hash; different rules fail closed to the existing baseline.

1. 2018 provides seed history. Fit on 2019–2022.
2. Choose among ridge penalties 10, 100 and 1000 using 2023 only.
3. Refit through 2023 with the selected penalty.
4. Calibrate central 80% residual intervals using 2024, without fitting on that year's outcomes.
5. Evaluate 2025 without selecting or changing coefficients against its results. The project previously inspected 2025 for older baselines; it is not a completely unseen project-level season.

Each feature uses strictly earlier weeks. Historical features never use today's player identity/team/depth metadata. Corrected public statistics are available, but archived original publication vintages are not. Evaluation includes only players with sufficient known history and a recorded target-window appearance. Consequently it does not test DNP/rookie prediction or the exact information available at every historical waiver deadline.

The uncertainty calculation was corrected during review to resample contiguous four-week blocks for overlapping four-week outcomes. No coefficient or hyperparameter tuning used the holdout. Nominal 80% intervals achieved about 76–83% coverage across the position/horizon cohorts. These are measured population-level results, not guaranteed player-specific probabilities. No head-to-head beat-your-replacement probability is presented.

## Lower-ranked-player results

Candidates are selected by **past baseline rank**, not their eventual scores: QB/TE ranks 13 onward and RB/WR ranks 25 onward, with bounded pool sizes. These are waiver-like research proxies—not reconstructed ESPN league ownership. Fixed groups of five create simulated keep-versus-switch choices. Regret measures points missed versus the best realized option; that hindsight winner is used only for evaluation, never for model selection of the player.

Mean absolute point error (lower is better):

| Position | Horizon | Observations | Historical baseline | Learned model | Fallback gate |
|---|---|---:|---:|---:|---|
| QB | One week | 385 | 6.821 | 6.737 | Not passed |
| RB | One week | 1,018 | 3.332 | 3.463 | Not passed |
| WR | One week | 1,718 | 3.524 | 3.552 | Not passed |
| TE | One week | 860 | 2.905 | 2.939 | Not passed |
| QB | Four weeks | 420 | 5.561 | 5.184 | Not passed: decision regret worsened |
| RB | Four weeks | 1,053 | 2.528 | 2.596 | Not passed |
| WR | Four weeks | 1,440 | 2.870 | 2.728 | Passed |
| TE | Four weeks | 720 | 2.315 | 2.216 | Passed |

Four-week WR mean absolute error improved 4.9%, TE 4.3%. In 285 WR and 135 TE proxy choice groups, the model-selected player's realized points per appearance averaged 0.44 and 0.15 more than the baseline-selected player's. These are research-cohort results, not promised roster gains. Horizon samples overlap and must not be summed as independent cases.

The predeclared gate requires at least 200 observations, at least 1% lower MAE, no worse RMSE or choice regret, at least 20 choice groups, a positive lower 95% paired block-bootstrap bound, and no worse all-player MAE. Better mean error alone is insufficient. No weekly model passed.

## What changes in the app

- Independent predictions, expected targets/carries, historical intervals and sample counts appear in player details and waiver explanations.
- **Forecast lab** on Waivers surfaces available, unlocked candidates using the four-week independent forecast. It shows up to two per position, or eight within the selected position, so high-scoring quarterbacks cannot crowd out every receiver or running back. This is a research list, not a legal add/drop ranking: need, replacement cost, injury context and budget still matter.
- ESPN weekly projections remain primary. Passing a historical-only comparison does not establish superiority over ESPN or its season projections.
- Qualified four-week WR/TE models can replace a **purely historical** baseline only when both ESPN weekly and season projections are absent and the comparison is inside that four-week window. They are not extrapolated through the rest of the season.
- The existing six-hour dataset workflow performs lightweight inference from committed, hash-verified parameters. It does not retrain itself against newly observed outcomes. Login/refresh shows source freshness and withholds mismatched forecasts.
- Frozen snapshots retain independent predictions, ESPN projections and the model identifier before outcomes. Validation history reports matched, pre-kickoff point errors versus ESPN once scores exist. Individual snapshot results are not independent season-wide evidence; repeated snapshots must be deduplicated before any aggregate superiority claim.
- No roster transactions, bids or trades are automated.

## Release verification — 2026-09-19

- Main implementation merged in [PR #10](https://github.com/Zelus7/fantasy-dvp-builder/pull/10), merge `5cc740663299afb5f92519276adc32a39b0d16bc`.
- Production refresh [35444838593](https://github.com/Zelus7/fantasy-dvp-builder/actions/runs/35444838593) succeeded: 535 forecasts targeting Week 2, 667 player features, 272 scheduled games, 15 roster players, 150 loaded waiver players, 14 available previous-week scores, advice ready and an immutable snapshot saved. Missing historical scores were not filled with zeroes.
- Calculation revision `waiver-plan-v3:f77d2c351d15`; forecast artifact `opportunity-v1:2514e8d194ca3e00`. Final Worker deployment including position-balanced research UI: `84c60d6f-ef53-4495-b045-02332358c6a5`.
- Regression checks: 102 JavaScript tests and 40 Python tests passed; Worker dry-run, syntax checks, secret scan and implementation GitHub CI passed. Additive D1 migration applied after a private backup.
- Signed-in Chrome: reload and explicit Refresh remained authenticated; ESPN actuals/projections remained separate from independent forecasts; unavailable-player warnings blocked model use; all three waiver horizons completed; five current-week archived decisions replayed as awaiting completed games. Older model archives are retained and covered by automated replay/fingerprint tests, not a new live historical outcome claim.
- Mobile 390×844: dashboard and expanded forecast lab had document width 390, with readable usage/interval evidence and no horizontal overflow. Browser error/warning log was empty during acceptance checks.
- No credentials changed, connector pairing repeated, or ESPN transactions submitted. These checks verify this release's behavior, not guaranteed forecasting superiority or a full connector re-pairing test.

## Test checklist

1. Reload and Refresh with Current week selected; remain signed in.
2. On Lineup, expand an offensive player's details and Independent forecast & expected workload. ESPN and independent estimates must remain separate.
3. Open Waivers → Forecast lab. Read the research-only warning and expand a player. Confirm target week, expected usage, interval coverage and model status.
4. Switch waiver horizons. Four-week points per appearance must not be labeled a four-week total or a season forecast.
5. Check injured, changed-team, unfamiliar and stale-data cases: warnings or missing estimates, never made-up zeroes.
6. Open validation history. This week's decisions await final scores; previous versions still replay from their archived calculations.

## Sources

- [nflverse data availability](https://nflreadr.nflverse.com/articles/nflverse_data_schedule.html)
- [Chronological split rationale](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html)
- Exact aggregate metrics and portable weights: `pipeline/models/opportunity-v1.json`.
