# Evidence-first waiver reviews

This release distinguishes an evidence-supported starting upgrade from a close
call, an unproven rebound, optional bench insurance, and an availability check.
These are decision-policy labels, not calibrated probabilities.

## Evidence and calculation boundaries

- ESPN exact-week point projections remain unchanged. Evidence does not add
  unvalidated projected-point bonuses.
- Kicker context uses identified, completed-game nflverse play-by-play: FG/PAT
  attempts and conversions, 50+ yard attempts, weekly volume and coverage. A
  zero-attempt game requires independently identified participation in weekly
  statistics. Missing results, stale data, team mismatches and incomplete
  participation cannot become convincing accuracy or opportunity evidence.
- Kicker storage records are context-only, not zero-point historical scores.
  They do not supply a fantasy scoring average or an independent kicker forecast.
- Current role samples, sources that disagree, availability, weak recent output,
  workload catalysts, touchdown dependence, drop cost and modeled starting weeks
  affect the review. Missing routes, practice, weather and independent kicker
  scoring/red-zone matchup analysis are disclosed, not presumed checked.
- The review's age is distinct from source timestamps. Each review includes a
  case against, keeping the roster as the baseline, and reconsideration triggers.

## Conservative policy, not fitted statistical cutoffs

Current evidence normally needs at least three observed games, through the latest
completed week, refreshed within eight days. A supported purchase also needs at
least two modeled starting points of benefit, a starting week, no adverse return
scenario, no questionable/conflicting availability, and no unexplained weak-score
or touchdown-dependence flag. Weak-score review means at least three recent scores
all below 75% of the current ESPN projection without an observed workload-growth
catalyst. These are explicit judgment rules; they have not been optimized or
validated as predictive thresholds.

Weekly differences below half a point are close calls. Material recent kicker
attempt differences (at least one attempt per game over two or more observed
games) may surface a close comparison, but never add points or force a switch.
Search remains bounded to 48 detailed pairs, with position coverage reserved.
The displayed coverage table is not a claim that every loaded player received
individual news research. Position filters narrow coverage.

## Spending and validation

Only supported reviews can receive an automatic conservative FAAB value-policy
range, after budget verification. Market history and competing-team needs remain
separate context, not winning-price predictions. Unsupported manual-review drafts
leave the exact bid blank even in leagues with a positive minimum. Saving a plan
does not execute any ESPN transaction.

Frozen outcomes default to holding for watch/insurance/close-call reviews and
compare supported moves against the frozen legal lineup and a projection-only
ESPN baseline. The model revision includes review code; previous replay archives
remain unchanged. This improves review discipline, not demonstrated predictive
superiority. Out-of-sample future results are still required to measure that.

## Verification and user checks

Automated cases cover the Shrader/Myers near-tie shape, a low-output Dobbins-like
stash, supported starters, stale/team-mismatched evidence, source conflicts,
crowded position screens, draft bids, aggregate corruption and frozen baselines.
The real public-feed build produced 32 identified kickers and passed normal
Worker ingestion validation. Its Shrader/Myers totals matched 11 and 5 FG
attempts respectively through week 4; this is a dated source check, not a forecast.

After deployment and a successful data refresh:

1. Refresh the app; expand Data freshness & coverage and look for Kicking.
2. In Waivers, inspect each category and open Why this move? Check the reasons
   against a move and the missing evidence, not just its point estimate.
3. Compare This week, Next 4 weeks and Rest of season; open week-by-week starters.
4. Filter K and inspect recent FG opportunity where a legal comparison exists.
   Keeping Shrader may correctly produce no supported replacement.
5. Draft a manual-review candidate: its exact bid must be blank. No ESPN claim
   should be created. Remove the unsaved draft when finished.
6. Check Review coverage; zero supported upgrades means no supported purchase,
   not that every possible waiver opportunity has been ruled out.
