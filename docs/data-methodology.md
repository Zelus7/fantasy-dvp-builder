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

The deterministic model starts from ESPN's weekly projection when available. If unavailable, it blends recent and season production. It then applies bounded adjustments for:

- Defense vs. position.
- Recent role and opportunity.
- Recent production trend.
- Injury status.
- Weather.

The adjusted median is expanded into floor and ceiling using observed weekly standard deviation or a conservative fallback spread. User-selected floor, median, and ceiling weights form the decision score.

## Lineup optimization

The app expands ESPN lineup slot counts into individual legal slots and solves a maximum-weight assignment problem. It respects ESPN eligible slots, FLEX/OP behavior, unique-player constraints, and games that have already kicked off.

## Waivers

Waiver value is the candidate's decision score minus the weakest rostered score at that position, adjusted modestly for positional need and rest-of-season matchup percentile. The app ranks only ESPN players marked FREEAGENT or WAIVERS.

## Trades

Private v1 is standard-redraft only. Trade value blends weekly decision value, season production, ceiling, rest-of-season matchup quality, fantasy-playoff matchup quality, and roster depth after the trade. The app reports whether a trade is close rather than manufacturing precision.

## Scoring limitations

Common passing, rushing, receiving, reception, turnover, chunk, and yardage-bonus scoring are supported. Rules that require play-level details absent from the weekly public feed—such as long-touchdown bonuses or some threshold overrides—are recorded under `unsupportedScoring`, shown in Settings, and reflected in lower confidence.
