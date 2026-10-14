# Data methodology

## Defense vs. Position

For each NFL defense, position, and week, the pipeline sums fantasy points scored by opposing QB, RB, WR, and TE players under the connected ESPN league's scoring settings. It then calculates:

- Points allowed per defense game.
- Season, last-four, and last-six windows.
- Rank from most to fewest points allowed.
- Position-relative percentile.
- League-average percentage difference.
- Recent direction: improving, stable, or worsening.
- Sample-size confidence.

Rank 1 means **most fantasy points allowed**, not best real-life defense. The UI spells that out to prevent rank-direction confusion.

## Early-season blending

Current data is noisy in September. Prior-season points allowed per game are blended with the current sample using:

| Current games | Prior-season weight |
|---:|---:|
| 0 | 100% |
| 1 | 75% |
| 2 | 60% |
| 3 | 45% |
| 4 | 30% |
| 5 | 15% |
| 6+ | 0% |

The UI shows current games, prior games, prior season, prior weight, and confidence. It never presents a one-game sample as settled truth.

## Player features

The pipeline builds league-scored season PPG, last-three and last-five PPG, standard deviation, targets, carries, touches, and ESPN/GSiS identity mappings. Early-season season PPG uses the same taper where prior data is available. Recent windows remain current-season first.

## Weekly outcomes

The selected-week point estimate uses ESPN's projection when available. It does
not add unvalidated matchup, role, injury or weather bonuses on top of it. These
inputs inform evidence and warnings. Without an ESPN projection, a qualified
learned fallback is used only inside its tested scope; otherwise the historical
season baseline is explicitly labeled experimental. Ruled-out and bye players
are unavailable for the current lineup.

Floor and ceiling use a heuristic spread, not calibrated probabilities.
User-selected floor, median and ceiling weights form the separate risk score.
Matching ESPN's central estimate is intentional; a different number alone is
not evidence of a better forecast.

## Lineup optimization

The app expands ESPN lineup slot counts into individual legal slots and solves a maximum-weight assignment problem. It respects ESPN eligible slots, FLEX/OP behavior, unique-player constraints, and games that have already kicked off.

## Waivers

The app compares the best legal starting lineup after an explicit add/drop with
the best legal lineup while holding. Bench projections are not added as starter
gain. Future calendar comparisons include byes, recovery bookends and drop costs;
healthy-game baselines are not exact future-week projections. Only players
marked FREEAGENT or WAIVERS in the verified pool may be considered.

Current-week insurance must identify a questionable player in the optimized
starting lineup. The app recomputes both holding and add/drop lineups with that
player unavailable, including existing backups and FLEX competition. At least
0.5 incremental starter points, legal timing, a usable estimate, a healthy
candidate and zero verified near-term drop cost are required for this insurance
screen. A missing absence probability stays missing; the conditional benefit
is never added to the normal forecast or used to manufacture paid-bid guidance.
Zero-gain bench additions without a demonstrated contingency are suppressed.
Future costs and incomplete evidence remain visible.

## Trades

The calendar-roster-fit-v2 screen separates two questions:

- **Bilateral fit:** both teams gain at least 0.5 starter points per week across
  the chosen window and every injury-return bookend, with no new starting-slot
  gaps. Capacity, protected players, locks, named extra drops and conditional
  additions are checked. No future waiver win is assumed.
- **Exchange value:** fill the league's legal starting slots, including FLEX,
  with healthy-game baselines. Compare each traded player's surplus over the
  best non-starting player at that position across available weeks. With no
  positional bench sample, use the lowest selected starter as a conservative
  boundary and label the context. Secondary package pieces count at 80%; the
  two package indexes must be within 25% in every return bookend. Zero-surplus
  or incomplete packages cannot establish balance.

The 0.5, 80% and 25% thresholds are screening policies, not calibrated market
prices. External market sentiment and acceptance probabilities are unverified.
Discovery examines up to 12 players per team with positional coverage, then up
to 32 balanced packages per counterpart, prioritizing fit and package diversity.
The full calendar model verifies shortlisted candidates. This is bounded, not
exhaustive. Trade sources are now included in the immutable model fingerprint.

## Scoring limitations

Common passing, rushing, receiving, reception, turnover, chunk, and yardage-bonus scoring are supported. Rules that require play-level details absent from the weekly public feed—such as long-touchdown bonuses or some threshold overrides—are recorded under `unsupportedScoring`, shown in Settings, and reflected in lower confidence.
