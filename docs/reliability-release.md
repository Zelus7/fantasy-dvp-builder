# Reliable decision support release

Status: implemented and deployed; scheduled refresh awaits authorization. This checklist is the durable work record; checked items
require evidence. Do not equate passing UI smoke tests with accurate advice.

## Scope and acceptance gates

- [x] ESPN actual and projected points select the exact season/week; zero and
  missing values remain distinct. Reconcile a real roster against ESPN.
- [x] Separate actual points, ESPN projection, model estimate and ranking.
  Support week selection and explain all derived metrics.
- [x] Audit all calculations for unavailable players, byes, locked games,
  early-season samples, double counting, missing data and unsupported scoring.
- [x] Backtest with chronological holdouts and baseline comparison; report
  errors and limitations without fabricated probability/confidence claims.
- [x] Weekly and rest-of-season waiver modes use distinct valuation. Show
  explanations, risks, legal add/drop alternatives, protected players and
  lineup impact. Reject unavailable weekly adds and harmful drops.
- [x] Trade discovery tests both teams' legal lineups, depth and future value,
  rejects lopsided exchanges, and explains benefit/cost for both sides. Support
  viable 1-for-1 and package candidates without claiming acceptance odds.
- [x] Dashboard prioritizes urgent roster problems, lineup improvements,
  waiver moves and bilateral trade opportunities; no move is a valid result.
- [x] Team accents/logos, mobile-friendly expandable explanations, clear
  status, matchup context and accessible labels across player cards.
- [x] Login/resume checks independent source freshness, retains last valid
  data, refreshes stale inputs and invalidates dependent recommendations.
- [ ] Scheduled statistics refresh actually runs and publishes validated
  snapshots. Failed/missing feeds remain visible and lower advice quality.
- [x] Verified injury/news coverage, timestamped source links and no claims
  of complete current information when a source is unavailable.
- [x] Watchlist/protected roster, changes since last visit, contingency
  replacements, bye/playoff depth planning and recommendation history.
- [x] Full regression suite, source-reconciliation checks, live browser
  smoke tests and narrow-screen visual review; no real ESPN transactions.
- [ ] Final review/polish, clean commit/push, deployment and post-deploy checks.

## Guardrails

Keep the current repository and branch. No unrelated Worker changes, no
automatic ESPN lineup/waiver/trade transactions, no unapproved paid services,
no secrets in output. Preserve installed connector pairing and app sessions.
Ask only for actual missing authority, credentials, or material product choices.

## Confirmed defects from initial audit

- Lineup renders decisionScore as an unlabeled score.
- ESPN stats parser takes the first matching split without season/week matching.
- OUT players have zero median but a positive heuristic ceiling/decision score.
- Waiver replacement is simply the weakest same-position score; no named drop,
  roster protection, legal lineup impact or distinct season-long objective.
- "Confidence" is a heuristic evidence score, not calibrated probability.
- League-wide trade lookup now loads, but valuation is not bilateral discovery.
- Dataset workflow registration/secrets were absent at the previous live audit.

## Work log

- Initial implementation audit and synthetic reproductions completed in the
  preceding conversation turn. Production code has not yet been changed for
  this release. Baseline branch commit: ed0065a.
- Work in progress: added exact-period `src/scoring.js`, wired it into ESPN
  normalization, added null/zero/negative/duplicate regression cases, and audited
  unavailable-player estimates and eligibility in `src/analysis.js`. 51 Node
  tests passed before the latest decision-engine addition.
- Added `src/decisions.js` for legal add/drop analysis, horizon valuation and
  bilateral 1-for-1 / 2-for-2 trade discovery. This module is not wired into API/UI
  and has not yet been tested. Its assumptions, runtime cost and edge cases must
  be reviewed before use. Nothing from this release has been deployed or pushed.
- User now explicitly requires Extra High (`xhigh`) and a re-review of all work
  since approving this release. Prior effective effort was not verified. Treat
  all current changes as provisional until that xhigh re-review and full tests.

### Extra High continuation, 2026-09-15

- Verified the active turn uses gpt-6-astra / xhigh from its local turn context.
  Re-reviewed the provisional parser, model and decision engine; no subagents.
- Added exact-period scoring, zero/negative/missing distinctions, authoritative
  eligibility, unavailable-player safeguards and exhaustive optimizer checks
  against 200 deterministic small rosters. Latest full Node suite: 65 passed;
  Python suite: 14 passed. `npm run ci` passed (before the browser-worker split;
  rerun before deployment). Logs are under `.artifacts/reliability-*`.
- New authenticated workspace/opportunities/trade-review/preferences/history
  service and UI are wired. Includes weekly/ROS explained add/drop cards,
  bilateral trade discovery, separate actual/projection/rank labels, week
  selector, watch/protect controls, source coverage, actions, contingencies,
  current-roster historical-score warning and bye/playoff schedule view.
- New team accents/logo metadata and mobile explanations. Local fixture browser
  testing has started at 390px; not equivalent to live ESPN reconciliation.
- Corrected postseason leakage, zero-point position games, tied DvP ranks,
  missing kickoff invention and feed-week metadata. Current rebuilt dataset:
  384 DvP rows / 661 feature rows / 272 scheduled games; not published yet.
- Completed chronological backtest: selected on 2024, evaluated on 2025 with
  prior-week-only inputs. Season baseline MAE 5.936 vs adjusted blend 6.351 on
  1,201 lineup-relevant holdout observations. Removed unvalidated forecast
  multipliers; details and limitations in `docs/model-validation.md`.
- Verified public ESPN headline feed; added cached source-linked news with
  publication timestamps and explicit incomplete injury coverage. Restored
  weather context; stale historical features no longer silently feed estimates.
- Heavy searches moved to a background browser worker using generated copies
  of the same pure tested model modules. No secrets/auth/database code is copied.
  Re-run full tests and verify browser execution after this change.
- GitHub read-only check: default branch is main; refresh workflow not registered;
  APP_BASE_URL and DATA_INGEST_TOKEN secret names absent. Asked Alex asynchronously
  for a scheduler-only main change (not merging app PR); awaiting response.
- Cloudflare login works; account billing endpoint is not authorized. No plan
  or billing changes made. Existing Worker usage model is standard.
- Release remains uncommitted, unpushed and undeployed at this checkpoint.
  Production remains prior recovery release. Still required: finish UI flows,
  malformed-input/freshness checks, build/deploy, real ESPN score reconciliation,
  scheduled refresh authorization/setup/run proof, live regression and final polish.

### Live verification and final review

- Published validated datasets: 272 regular-season games, 384 DvP rows and
  661 player feature rows. Actual source coverage is week 1. Previous snapshots
  remain available; no credential, pairing or login secret was changed.
- Signed-in ESPN clubhouse reconciliation: all 15 roster actual scores matched
  exactly and all 15 projections matched ESPN's displayed precision. Starter
  total matched at 89.22. Normalized season projections exist for all 15 players.
  Evidence: ignored `.artifacts/espn-ui-reconciliation.json` and
  `.artifacts/live-score-reconciliation.json`. No raw cookies/responses exposed.
- Preserved two decimal places for actual points; separate exact-week projection
  and scenario-weighted rank score. Live reload/session and week-2 planning work.
- Live week-2 check found incorrect textual pairings (QB start paired with WR
  bench); fixed slot-aware pairing and added a regression test. The optimized
  lineup was legal; action explanations now follow that arrangement.
- Current suite: 73 JavaScript tests, 14 Python tests; Worker dry-run and secret
  scan pass. Includes real-SQLite authenticated preference/history routes,
  deduplicated recommendation snapshots, historical advice suppression, source
  freshness, bye coverage and malformed trade requests.
- Mobile synthetic browser check at 390px: no horizontal page overflow. Manual
  balanced-trade form returned +10/+10 in the fixture. External logo failure
  falls back to team-color accents. The isolated fixture browser/server stopped.
- ESPN blocks the server news request; verified its public CORS endpoint and
  added a credential-free browser fallback. Live source-linked headlines load;
  roster mentions are prioritized. Complete injury/inactive coverage is not
  claimed. Freshness for statistics, DvP and future schedule is independent.
- Heavy search uses the same generated pure model modules in a background browser
  worker. Form views preserve unsent selections when data becomes stale. Critical
  refreshes temporarily disable old controls to avoid acting on mixed weeks.
- Added week-by-week bye coverage gaps and saved waiver/trade recommendation
  snapshots (not executed actions). Neither the app nor these tests made an
  ESPN roster, waiver or trade transaction.
- Refresh workflow prepared for six-hour polling with explicit configuration
  failures and validated dispatch inputs. It is NOT registered on main and its
  ingestion secrets are NOT configured. Await Alex's scheduler-only approval;
  do not merge PR #6 or change the default branch to bypass this gate.
