import copy
import unittest
import numpy as np
import pandas as pd
from pipeline.forecast import (FEATURES, vector, normalized_rows, examples, fit,
                               predict, artifact_hash, promotion, pool, choices)
from pipeline.build_datasets import score_row
from pipeline.build_datasets import scoring_hash
from pipeline.forecast import load_artifact, enrich_forecasts

ITEMS = [{'statId': 3, 'points': .04}, {'statId': 20, 'points': -2},
         {'statId': 53, 'points': .5}, {'statId': 42, 'points': .1},
         {'statId': 72, 'points': -2}]


def sample():
    return normalized_rows(pd.DataFrame([
        {'season': 2024, 'week': w, 'season_type': 'REG', 'player_id': 'a',
         'position': 'WR', 'team': 'SEA', 'targets': w+2, 'receptions': w,
         'receiving_yards': w*10} for w in range(1, 9)]), ITEMS)


class ForecastTests(unittest.TestCase):
    def test_modern_interceptions_and_fumbles(self):
        actual, _ = score_row({'passing_yards': 250, 'passing_interceptions': 2,
                              'fumbles_lost_total': 1, 'sack_fumbles_lost': 1}, ITEMS)
        self.assertEqual(actual, 4)
        self.assertEqual(score_row({'interceptions': 1, 'fumbles_lost': 1}, ITEMS)[0], -4)

    def test_zero_primary_alias_is_not_overridden(self):
        self.assertEqual(score_row({'passing_interceptions': 0, 'interceptions': 3}, ITEMS)[0], 0)

    def test_future_rows_and_target_outcome_do_not_enter_features(self):
        rows = sample()
        before = vector(rows, 2024, 5)
        altered = copy.deepcopy(rows)
        for row in altered:
            if row['week'] >= 5:
                row['points'] = 1000
                row['targets'] = 1000
                row['position'] = 'QB'
        np.testing.assert_array_equal(before, vector(altered, 2024, 5))
        self.assertEqual(len(before), len(FEATURES))
        a = next(r for r in examples(rows, [2024]) if r['week'] == 5)
        b = next(r for r in examples(altered, [2024]) if r['week'] == 5)
        self.assertEqual(a['x'], b['x'])
        self.assertNotEqual(a['y'], b['y'])
        self.assertEqual(b['position'], 'WR')

    def test_insufficient_history_is_missing_not_fabricated(self):
        self.assertIsNone(vector(sample(), 2024, 3))
        self.assertIsNone(vector(sample(), 2026, 3))

    def test_share_denominator_is_player_team_week(self):
        rows = normalized_rows(pd.DataFrame([
            {'player_id': 'a', 'position': 'WR', 'team': 'SEA', 'season': 2024, 'week': 1, 'targets': 5},
            {'player_id': 'b', 'position': 'TE', 'team': 'SEA', 'season': 2024, 'week': 1, 'targets': 5},
            {'player_id': 'a', 'position': 'WR', 'team': 'SEA', 'season': 2024, 'week': 2, 'targets': 1},
        ]), ITEMS)
        self.assertEqual(rows[0]['target_share'], .5)
        self.assertEqual(rows[-1]['target_share'], 1)

    def test_four_week_target_is_observed_average_not_zero_imputation(self):
        rows = [r for r in sample() if r['week'] != 6]
        result = next(r for r in examples(rows, [2024], 4) if r['week'] == 5)
        self.assertEqual(result['appearances'], 3)
        self.assertEqual(result['y'][0], np.mean([r['points'] for r in rows if 5 <= r['week'] <= 8]))

    def test_duplicate_rows_rejected(self):
        row = {'season': 2024, 'week': 1, 'player_id': 'a', 'position': 'RB'}
        with self.assertRaises(ValueError):
            normalized_rows(pd.DataFrame([row, row]), ITEMS)

    def test_portable_ridge_handles_constant_features(self):
        rows = [{'x': [float(i), 1.], 'y': [i*2., i, 0.]} for i in range(30)]
        model = fit(rows, 10)
        result = predict(model, [[10., 1.]])
        self.assertTrue(np.isfinite(result).all())
        self.assertLess(abs(result[0][0]-20), 3)

    def test_promotion_fails_closed_on_weak_or_negative_evidence(self):
        metrics = {'n': 300, 'model': {'mae': 4, 'rmse': 6}, 'baseline': {'mae': 5, 'rmse': 7}}
        decision = {'choices': 30, 'modelRegret': 2, 'baselineRegret': 3}
        self.assertTrue(promotion(metrics, decision, [.1, .3]))
        self.assertFalse(promotion(metrics, decision, [-.1, .3]))
        self.assertFalse(promotion(metrics, {**decision, 'modelRegret': 4}, [.1, .3]))
        self.assertFalse(promotion({**metrics, 'n': 20}, decision, [.1, .3]))

    def test_pool_membership_does_not_use_actual_points(self):
        rows = [{'season': 2025, 'week': 3, 'position': 'WR', 'id': str(i),
                 'baseline': float(i), 'y': [float(i)], 'prediction': [float(i)]} for i in range(60)]
        ids = [r['id'] for r in pool(rows)]
        changed = [{**r, 'y': [999-r['y'][0]]} for r in rows]
        self.assertEqual(ids, [r['id'] for r in pool(changed)])
        self.assertEqual(choices(rows)['gainVsHold'], 0)

    def test_artifact_hash_covers_weights_and_gates(self):
        a = {'models': {'WR': {'approvedFallback': False}}, 'features': FEATURES}
        self.assertNotEqual(artifact_hash(a), artifact_hash({**a, 'models': {}}))
        self.assertEqual(artifact_hash(a), artifact_hash({**a, 'version': 'ignored'}))

    def test_live_inference_filters_target_week_before_computing_features(self):
        artifact=copy.deepcopy(load_artifact())
        artifact['scoringHash']=scoring_hash(ITEMS)
        stats=pd.DataFrame([{'season':2025,'week':w,'player_id':'a','position':'WR',
                            'team':'SEA','targets':5,'receptions':3,'receiving_yards':40}
                           for w in range(1,9)]+[
                            {'season':2026,'week':1,'player_id':'a','position':'WR','team':'SEA','targets':6},
                            {'season':2026,'week':2,'player_id':'a','position':'WR','team':'SEA','targets':999}])
        a=[{'gsisId':'a','position':'WR'}];b=copy.deepcopy(a)
        enrich_forecasts(a,stats,ITEMS,2026,2,artifact)
        stats.loc[stats.season==2026,'receiving_yards']=0
        stats.loc[(stats.season==2026)&(stats.week==2),'receiving_yards']=9999
        enrich_forecasts(b,stats,ITEMS,2026,2,artifact)
        self.assertEqual(a[0]['forecast'],b[0]['forecast'])
        self.assertEqual(a[0]['forecast']['lastObservedWeek'],1)

    def test_inference_scoring_mismatch_removes_old_forecasts(self):
        features=[{'forecast':{'old':True}}]
        result=enrich_forecasts(features,pd.DataFrame(),[],2026,2,load_artifact())
        self.assertEqual(result['status'],'scoring-mismatch')
        self.assertIsNone(features[0]['forecast'])


if __name__ == '__main__':
    unittest.main()
