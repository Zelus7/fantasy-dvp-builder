# Waiver planning v3

## What changed

- Three horizons: this week, next four weeks / injury bridge, and rest of season through the configured fantasy playoffs.
- Each named add/drop is compared with keeping the roster and using its best legal starting lineup. Future weeks include actual byes and explicit injury-return scenarios. The arbitrary 15% bench-value bonus is no longer used by the waiver page.
- Weekly streams with a future cost are labeled **short-term tradeoffs**, not silently rejected or described as unqualified upgrades.
- Injury hold reviews compare retaining the injured player with a named available alternative. IR capacity and eligibility are separate from the NFL injury designation. Unknown timing is a range of assumptions, not a medical forecast.
- Source-reviewed return windows expire after three days. Refresh never extends their verification date. The initial Brown record uses the Patriots' September 12 report for Week 6 eligibility; Weeks 7 and 9 are explicitly hypothetical stress tests.
- Current-season team target share, carry share, recent targets, target trend, offensive snaps, scoring-area opportunities and depth rank are available where the public feeds support them. Small samples and touchdown-dependent production are called out.
- Bid guidance requires a current, verified FAAB balance and minimum bid. Spending caps are conservative policy rules, not calibrated win odds. At least five identifiable executed position-matched waiver bids are needed to display a historical market median.
- Fallbacks include a named existing bench option and alternative claims. Decision deadlines account for the add, drop and fallback players' kickoff times. An unknown ESPN processing deadline remains unknown.
- Server-owned immutable decision inputs include the candidate pool, source freshness, exact model-code fingerprint, league constraints and return assumptions. Frozen lineups can later be scored against exact-week ESPN results and an ESPN-only weekly baseline; no hindsight optimization or fabricated zeroes.
- Refresh forces the roster, waiver pool and current news checks. Historical-week selection now explains why advice is withheld. Player-specific ESPN headlines supplement the general feed.

## Deliberate limits

This is not yet a validated predictive edge. Future totals repeat healthy-game baselines through known schedule/availability scenarios; they are not independently calibrated weekly forecasts. Opportunity evidence explains risk and role but does not secretly multiply ESPN projections.

The candidate pool is capped at 150 ESPN ownership-ranked available players. Every legal pair is screened; at most two pairs per player and 48 pairs total receive the more expensive multiweek evaluation. This is disclosed and is not a claim to exhaust the waiver market.

No complete live practice/inactive feed is available from the current free pipeline. Player headlines are source-linked context; return scenarios require review. Public participation/route data is not represented as a live feed. See the [nflverse availability schedule](https://nflreadr.nflverse.com/articles/nflverse_data_schedule.html).

Historical bid data is used only if ESPN explicitly supplies executed bids with amounts. Missing or rejected transactions do not become zero-dollar wins. Claim execution and actual acquisition cost are not inferred from viewing a recommendation. Validation history lets you record an acquired, missed or skipped claim and actual spend; these remain explicitly user-reported, not ESPN-verified.

No ESPN roster moves, bids or trade offers are submitted by the app or this release.

## Verification

- JavaScript tests: 90 passing at the deployment gate.
- Python tests: 26 passing at the deployment gate.
- Worker dry run, syntax checks and secret scan passed.
- Enhanced public dataset build: 272 schedule games, 384 DvP rows, 667 player features; 360 current-season workload records, 359 matched snap records, 360 scoring-area records. Statistical coverage is Week 1, not the requested future Week 2.
- Brown hold, Pierce/Robinson earlier deadline, reserve capacity, unavailable/locked/protected players, missing data, budget caps, immutable inputs and future-cost tradeoffs have regression coverage.
- Isolated mobile preview tested at 390 × 844 with no horizontal overflow. External logo requests were blocked in the isolated browser; text identities and team colors remained available.
- Existing signed-in Chrome session could not be inspected due to its URL policy. Do not represent the isolated preview as a completed live-session walkthrough.

Production migration, deployment, scheduled publication and live endpoint verification results are recorded below when completed.

## User acceptance checklist

1. Reload the command center once. Keep **Current week** selected and press Refresh. It should not log you out or repeatedly tell you to refresh a historical selection.
2. On Waivers, open the explanation of the former Waiver Edge. Compare **Starter improvement** with the named drop and your existing bench alternative.
3. Switch among This week, Four-week bridge and Rest of season. Expand a recommendation's week-by-week comparison; future totals and tradeoffs should be clear.
4. Review Brown's hold card: no league IR slot should be invented; Week 6 is earliest eligibility, not a promised recovery date. Review source dates before relying on scenarios.
5. Check Pierce's contingency against Robinson's earlier kickoff. Confirm actual waiver processing deadlines in ESPN.
6. Inspect the bid range, hard cap, remaining balance and history sample warning. Do not place all alternative claims as independent acquisitions.
7. Review source timestamps and workload evidence. Missing data should say missing; one game's usage should not be described as an established trend.
8. Open validation history. New decisions should await completed weeks. Later scores are counterfactual comparisons, not proof that a claim was acquired. Record what you actually did and the actual FAAB cost when known.
