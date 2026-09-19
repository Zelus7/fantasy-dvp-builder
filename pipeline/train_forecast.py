"""Reproducible, chronological opportunity forecast training and release gate."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
from .build_datasets import load_nflreadpy, scoring_hash
from .forecast import (FEATURES, TARGETS, POSITIONS, normalized_rows, examples, fit,
                       predict, pool, score_metrics, choices, paired_week_bounds,
                       promotion, artifact_hash)


def with_predictions(rows, model):
    values = predict(model, [r['x'] for r in rows])
    return [{**r, 'prediction': p.tolist()} for r, p in zip(rows, values)]


def train(stats, items):
    rows = normalized_rows(stats, items)
    artifact = {'schema': 1, 'features': FEATURES, 'targets': TARGETS,
                'scoringHash': scoring_hash(items), 'models': {},
                'protocol': {'fit': [2019, 2020, 2021, 2022], 'selection': 2023,
                             'refitThrough': 2023, 'calibration': 2024, 'evaluation': 2025},
                'limitations': [
                    'Conditional on recording a stat line; missing appearances are not zeroes.',
                    'No historical ESPN projections, pre-deadline news, actual waiver pools or bids.',
                    '2025 was previously inspected for older baselines; no new tuning uses it.',
                    'Four-week target is mean points per recorded appearance, not total roster value.',
                    'Stats are corrected historical records, not archived original publication vintages.',
                    'Current snap/depth/red-zone context is not a fitted feature in this version.',
                    'No promised injury recovery, transaction success or ESPN superiority.']}
    evidence = {}
    for horizon in (1, 4):
        data = examples(rows, range(2019, 2026), horizon)
        print(json.dumps({'phase': 'features', 'horizon': horizon, 'examples': len(data)}), flush=True)
        for pos in POSITIONS:
            group = [r for r in data if r['position'] == pos]
            training = [r for r in group if r['season'] <= 2022]
            selection = [r for r in group if r['season'] == 2023]
            calibration = [r for r in group if r['season'] == 2024]
            holdout = [r for r in group if r['season'] == 2025]
            scores = []
            for penalty in (10., 100., 1000.):
                candidate = fit(training, penalty)
                evaluated = with_predictions(selection, candidate)
                scores.append((score_metrics(pool(evaluated))['model']['mae'], penalty))
            penalty = min(scores)[1]
            model = fit(training+selection, penalty)
            cal = with_predictions(calibration, model)
            residuals = np.array([r['y'][0]-r['prediction'][0] for r in cal])
            # Fixed empirical central interval. Coverage is measured, not assumed.
            q10, q90 = np.quantile(residuals, [.1, .9])
            tested = with_predictions(holdout, model)
            cohort = pool(tested)
            metrics, decision = score_metrics(cohort), choices(tested)
            bounds = paired_week_bounds(cohort, block_length=horizon)
            all_metrics = score_metrics(tested)
            approved = promotion(metrics, decision, bounds) and (
                all_metrics['model']['mae'] <= all_metrics['baseline']['mae'])
            coverage = float(np.mean([q10 <= r['y'][0]-r['prediction'][0] <= q90 for r in tested]))
            model.update(approvedFallback=approved, evaluation=metrics, allEvaluation=all_metrics,
                         decisions=decision, pairedWeeklyMaeImprovement95=bounds,
                         calibration={'q10': float(q10), 'q90': float(q90),
                                      'count': len(cal), 'holdoutCoverage': round(coverage, 4)},
                         selection=[{'penalty': p, 'mae': m} for m, p in scores])
            key = f'{pos}:{horizon}'
            artifact['models'][key] = model
            evidence[key] = [{k: v for k, v in r.items() if k != 'x'} for r in tested]
            print(json.dumps({'model': key, 'approvedFallback': approved, 'metrics': metrics,
                              'choices': decision, 'intervalCoverage': coverage,
                              'pairedMae95': bounds}), flush=True)
    artifact['version'] = artifact_hash(artifact)
    return artifact, evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', required=True)
    parser.add_argument('--output-dir', default='.artifacts/forecast-v1')
    parser.add_argument('--artifact', default='pipeline/models/opportunity-v1.json')
    args = parser.parse_args()
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    items = json.loads(Path(args.config_file).read_text())['leagues'][0]['scoringItems']
    frames = []
    for season in range(2018, 2026):
        path = output/f'stats-{season}.parquet'
        if not path.exists():
            frame = load_nflreadpy('load_player_stats', [season])
            frame.to_parquet(path)
        frames.append(pd.read_parquet(path))
        print(json.dumps({'season': season, 'rows': len(frames[-1])}), flush=True)
    artifact, evidence = train(pd.concat(frames, ignore_index=True), items)
    dest = Path(args.artifact)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(artifact, sort_keys=True, indent=2, allow_nan=False)+'\n')
    (output/'holdout-predictions.json').write_text(json.dumps(evidence, allow_nan=False)+'\n')
    (output/'report.json').write_text(json.dumps({
        **{k: v for k, v in artifact.items() if k != 'models'},
        'models': {k: {a: b for a, b in v.items() if a not in ('center', 'scale', 'coefficients')}
                   for k, v in artifact['models'].items()}}, indent=2, allow_nan=False)+'\n')
    print(json.dumps({'complete': True, 'version': artifact['version'], 'artifact': str(dest)}), flush=True)


if __name__ == '__main__':
    main()
