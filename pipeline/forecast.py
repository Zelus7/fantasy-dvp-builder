"""Leakage-safe opportunity features and portable regularized forecasts.

Only historical stat lines enter the features. Targets are conditional on a
recorded appearance; missing games are not claimed as known DNPs or zeroes.
Training lives outside the Worker. Inference needs numpy, not sklearn or an API.
"""
from __future__ import annotations
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd

POSITIONS = ('QB', 'RB', 'WR', 'TE')
MEASURES = ('points', 'targets', 'carries', 'attempts', 'receptions',
            'receiving_yards', 'rushing_yards', 'passing_yards',
            'receiving_tds', 'rushing_tds', 'passing_tds',
            'target_share', 'carry_share', 'air_yards_share')
FEATURES = [f'{stat}_{window}' for stat in MEASURES
            for window in ('baseline', 'recent3', 'last')]
FEATURES += ['current_games', 'prior_games', 'gap_weeks', 'week', 'points_sd']
TARGETS = ('points', 'targets', 'carries')
STARTERS = {'QB': 12, 'RB': 24, 'WR': 24, 'TE': 12}
POOL_LIMITS = {'QB': 40, 'RB': 96, 'WR': 120, 'TE': 60}
MODEL_PATH = Path(__file__).with_name('models') / 'opportunity-v1.json'


def finite(value, default=0.):
    try:
        v = float(value)
        return v if np.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def normalized_rows(stats, items):
    if __package__:
        from .build_datasets import score_row
    else:
        from build_datasets import score_row
    result = []
    totals = defaultdict(lambda: [0., 0.])
    for r in stats.to_dict('records'):
        if r.get('season_type', 'REG') != 'REG' or r.get('position') not in POSITIONS:
            continue
        pid = r.get('player_id')
        if not pid:
            continue
        season, week = int(r['season']), int(r['week'])
        if not 1 <= week <= 18:
            continue
        team = r.get('team') or r.get('recent_team')
        row = {key: finite(r.get(key)) for key in MEASURES}
        row.update(id=str(pid), position=r['position'], season=season, week=week,
                   team=team, points=score_row(r, items)[0])
        result.append(row)
        totals[(season, week, team)][0] += row['targets']
        totals[(season, week, team)][1] += row['carries']
    seen = set()
    for r in result:
        key = (r['season'], r['week'], r['id'])
        if key in seen:
            raise ValueError('Duplicate player-week: aggregate upstream, never double-count')
        seen.add(key)
        t, c = totals[(r['season'], r['week'], r['team'])]
        r['target_share'] = r['targets'] / t if t else 0.
        r['carry_share'] = r['carries'] / c if c else 0.
    return sorted(result, key=lambda r: (r['season'], r['week'], r['id']))


def vector(history, season, week):
    if __package__:
        from .build_datasets import prior_weight
    else:
        from build_datasets import prior_weight
    past = [r for r in history if r['season'] == season-1 or
            (r['season'] == season and r['week'] < week)]
    if len(past) < 3:
        return None
    past = sorted(past, key=lambda r: (r['season'], r['week']))
    current = [r for r in past if r['season'] == season]
    prior = [r for r in past if r['season'] == season-1]
    weight = prior_weight(len(current)) if prior else 0.
    sample = current or prior
    values = []
    for stat in MEASURES:
        a = np.mean([r[stat] for r in current]) if current else 0.
        b = np.mean([r[stat] for r in prior]) if prior else 0.
        values += [a*(1-weight)+b*weight,
                   np.mean([r[stat] for r in sample[-3:]]), sample[-1][stat]]
    values += [len(current), len(prior),
               min(18, week-current[-1]['week']-1) if current else 0,
               week, np.std([r['points'] for r in sample])]
    return np.asarray(values, dtype=float)


def examples(rows, seasons, horizon=1):
    """Target window is never passed to vector; target membership is explicit."""
    histories = defaultdict(list)
    for r in rows:
        histories[r['id']].append(r)
    output = []
    for pid, history in histories.items():
        for season in seasons:
            season_rows = [r for r in history if r['season'] == season]
            if not season_rows:
                continue
            last_week = 18 if season >= 2021 else 17
            for week in range(1, last_week-horizon+2):
                x = vector(history, season, week)
                if x is None:
                    continue
                target = [r for r in season_rows if week <= r['week'] < week+horizon]
                if not target:
                    continue  # No appearance outcome, not an assumed zero.
                known = [r for r in history if r['season'] == season-1 or
                         (r['season'] == season and r['week'] < week)]
                pos = known[-1]['position']
                output.append(dict(id=pid, season=season, week=week, position=pos,
                                   x=x.tolist(), baseline=float(x[0]), appearances=len(target),
                                   y=[float(np.mean([r[k] for r in target])) for k in TARGETS]))
    return output


def fit(rows, penalty):
    x, y = np.array([r['x'] for r in rows]), np.array([r['y'] for r in rows])
    center, scale = x.mean(axis=0), x.std(axis=0)
    scale[scale < 1e-8] = 1.
    a = np.column_stack([np.ones(len(x)), (x-center)/scale])
    regularizer = np.eye(a.shape[1])*penalty
    regularizer[0, 0] = 0.
    coefficients = np.linalg.solve(a.T@a+regularizer, a.T@y)
    return {'center': center.tolist(), 'scale': scale.tolist(),
            'coefficients': coefficients.tolist(), 'penalty': penalty, 'trainingCount': len(rows)}


def predict(model, x):
    x = np.asarray(x, dtype=float)
    a = np.column_stack([np.ones(len(x)), (x-model['center'])/model['scale']])
    p = a@np.asarray(model['coefficients'])
    p[:, 0] = np.clip(p[:, 0], -10, 65)
    p[:, 1:] = np.clip(p[:, 1:], 0, 50)
    return p


def pool(rows):
    """Past-baseline ranks only; never called an actual ESPN available pool."""
    groups = defaultdict(list)
    for r in rows:
        groups[(r['season'], r['week'], r['position'])].append(r)
    out = []
    for (_, _, pos), group in groups.items():
        out.extend(sorted(group, key=lambda r: (-r['baseline'], r['id']))[
            STARTERS[pos]:POOL_LIMITS[pos]])
    return out


def score_metrics(rows):
    if not rows:
        return {'n': 0}
    actual = np.array([r['y'][0] for r in rows])
    def stats(p):
        error = np.asarray(p)-actual
        return {'mae': round(float(np.mean(abs(error))), 4),
                'rmse': round(float(np.sqrt(np.mean(error**2))), 4),
                'bias': round(float(np.mean(error)), 4)}
    return {'n': len(rows), 'baseline': stats([r['baseline'] for r in rows]),
            'model': stats([r['prediction'][0] for r in rows])}


def choices(rows):
    """Fixed groups of five similar-position lower-ranked options; hold=baseline pick."""
    groups = defaultdict(list)
    for r in pool(rows):
        groups[(r['season'], r['week'], r['position'])].append(r)
    gains, model_regret, base_regret, losses = [], [], [], []
    for group in groups.values():
        # ID order is independent of scores; one disjoint set of choices per week.
        ordered = sorted(group, key=lambda r: r['id'])
        for offset in range(0, len(ordered)-4, 5):
            options = ordered[offset:offset+5]
            hold = max(options, key=lambda r: (r['baseline'], r['id']))
            selected = max(options, key=lambda r: (r['prediction'][0], r['id']))
            best = max(r['y'][0] for r in options)
            gain = selected['y'][0]-hold['y'][0]
            gains.append(gain)
            model_regret.append(best-selected['y'][0])
            base_regret.append(best-hold['y'][0])
            losses.append(selected['id'] != hold['id'] and gain < -5)
    return {'choices': len(gains),
            'gainVsHold': round(float(np.mean(gains)), 4) if gains else None,
            'modelRegret': round(float(np.mean(model_regret)), 4) if gains else None,
            'baselineRegret': round(float(np.mean(base_regret)), 4) if gains else None,
            'costlySwitchRate': round(float(np.mean(losses)), 4) if gains else None}


def paired_week_bounds(rows, block_length=1):
    groups = defaultdict(list)
    for r in rows:
        groups[(r['season'], r['week'])].append(
            abs(r['baseline']-r['y'][0])-abs(r['prediction'][0]-r['y'][0]))
    weeks = [groups[k] for k in sorted(groups)]
    if len(weeks) < 10:
        return [None, None]
    rng = np.random.default_rng(20260919)
    # Adjacent four-week labels overlap. Resample contiguous four-week blocks
    # rather than incorrectly treating those weekly errors as independent.
    draws = []
    for _ in range(1000):
        starts = rng.integers(0, len(weeks)-block_length+1,
                              int(np.ceil(len(weeks)/block_length)))
        indices = [i for start in starts for i in range(start, start+block_length)][:len(weeks)]
        draws.append(np.mean([v for i in indices for v in weeks[i]]))
    return np.quantile(draws, [.025, .975]).round(4).tolist()


def promotion(metrics, decision, bounds):
    return bool(metrics['n'] >= 200 and
                metrics['model']['mae'] <= metrics['baseline']['mae']*.99 and
                metrics['model']['rmse'] <= metrics['baseline']['rmse'] and
                decision['choices'] >= 20 and
                decision['modelRegret'] <= decision['baselineRegret'] and
                bounds[0] is not None and bounds[0] > 0)


def artifact_hash(artifact):
    body = {k: v for k, v in artifact.items() if k != 'version'}
    return 'opportunity-v1:'+hashlib.sha256(json.dumps(body, sort_keys=True,
             separators=(',', ':'), allow_nan=False).encode()).hexdigest()[:16]


def load_artifact(path=MODEL_PATH):
    artifact = json.loads(Path(path).read_text())
    if artifact.get('version') != artifact_hash(artifact) or artifact.get('features') != FEATURES:
        raise ValueError('Forecast artifact hash/schema mismatch')
    return artifact


def enrich_forecasts(features, stats, items, season, target_week, artifact=None):
    if __package__:
        from .build_datasets import scoring_hash
    else:
        from build_datasets import scoring_hash
    artifact = artifact or load_artifact()
    for feature in features:
        feature['forecast'] = None
    if scoring_hash(items) != artifact['scoringHash']:
        return {'status': 'scoring-mismatch', 'rows': 0, 'version': artifact['version']}
    history = defaultdict(list)
    # Filter before any computation, including team share denominators.
    past = stats[(stats.season == season-1) | ((stats.season == season) & (stats.week < target_week))]
    for row in normalized_rows(past, items):
        history[row['id']].append(row)
    count = 0
    for feature in features:
        rows = history.get(feature['gsisId'], [])
        x = vector(rows, season, target_week)
        pos = feature['position']
        if x is None or pos not in POSITIONS:
            feature['forecast'] = None
            continue
        horizons = {}
        for horizon in (1, 4):
            if horizon == 4 and target_week > 15:
                continue
            model = artifact['models'][f'{pos}:{horizon}']
            values = predict(model, [x])[0]
            calibration = model['calibration']
            horizons[str(horizon)] = {
                'points': round(float(values[0]), 2), 'targets': round(float(values[1]), 2),
                'carries': round(float(values[2]), 2),
                'low': round(float(values[0]+calibration['q10']), 2),
                'high': round(float(values[0]+calibration['q90']), 2),
                'nominalCoverage': .8, 'measuredCoverage': calibration['holdoutCoverage'],
                'calibrationCount': calibration['count'], 'holdoutCount': model['evaluation']['n'],
                'intervalTestCount': model['allEvaluation']['n'],
                'approvedFallback': model['approvedFallback'],
                'mae': model['evaluation']['model']['mae'],
                'baselineMae': model['evaluation']['baseline']['mae'],
            }
        feature['forecast'] = {'version': artifact['version'], 'season': season,
            'targetWeek': target_week, 'throughWeek': target_week-1,
            'lastObservedWeek': rows[-1]['week'], 'lastObservedSeason': rows[-1]['season'],
            'historicalTeam': rows[-1]['team'], 'historyGames': len(rows),
            'currentGames': int(x[-5]), 'horizons': horizons,
            'trainedThrough': 2023, 'calibrationSeason': 2024, 'evaluationSeason': 2025,
            'conditionalOnAppearance': True, 'scoringHash': artifact['scoringHash']}
        count += 1
    return {'status': 'ready', 'rows': count, 'version': artifact['version'], 'targetWeek': target_week}
