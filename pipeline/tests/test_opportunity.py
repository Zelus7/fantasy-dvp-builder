import unittest
import pandas as pd
from pipeline.opportunity import enrich_opportunity


class OpportunityTests(unittest.TestCase):
    def fixture(self, snaps=None, pbp=None):
        features = [{'gsisId': 'p1', '_weeklyPoints': [{'week': 1, 'points': 8.5}]}]
        current = pd.DataFrame([
            {'player_id': 'p1', 'week': 1, 'team': 'BUF', 'targets': 5, 'carries': 2, 'receptions': 3, 'receiving_tds': 1},
            {'player_id': 'p2', 'week': 1, 'team': 'BUF', 'targets': 15, 'carries': 18},
        ])
        players = pd.DataFrame([{'gsis_id': 'p1', 'pfr_id': 'pfr1'}])
        return enrich_opportunity(features, current, players, pd.DataFrame(snaps or []), pd.DataFrame(pbp or []), pd.DataFrame(), 1)[0]

    def test_shares_use_team_denominators_and_missing_stays_missing(self):
        row = self.fixture()['opportunity']
        self.assertEqual(row['targetShare'], .25)
        self.assertEqual(row['carryShare'], .1)
        self.assertIsNone(row['snapShare'])
        self.assertIsNone(row['redZoneOpportunities'])
        self.assertIsNone(row['targetTrend'])
        self.assertTrue(row['touchdownDependent'])
        self.assertEqual(row['weeklyPoints'][0]['points'], 8.5)

    def test_snap_join_and_redzone_are_observed_not_imputed(self):
        row = self.fixture(snaps=[{'pfr_player_id': 'pfr1', 'week': 1, 'offense_pct': .6}], pbp=[
            {'posteam': 'BUF', 'week': 1, 'yardline_100': 12, 'receiver_player_id': 'p1', 'pass_attempt': 1},
            {'posteam': 'BUF', 'week': 1, 'yardline_100': 5, 'receiver_player_id': 'p1', 'pass_attempt': 1, 'no_play': 1}
        ])['opportunity']
        self.assertEqual(row['snapShare'], .6)
        self.assertEqual(row['redZoneOpportunities'], 1)

    def test_prior_only_player_has_no_current_season_opportunity(self):
        row = enrich_opportunity([{'gsisId': 'absent'}], pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 1)[0]
        self.assertIsNone(row['opportunity'])

    def test_invalid_weekly_snap_percentage_remains_unknown(self):
        row = self.fixture(snaps=[{'pfr_player_id':'pfr1','week':1,'offense_pct':60}])['opportunity']
        self.assertIsNone(row['snapShare'])
        self.assertIsNone(row['weeklyUsage'][0]['snapShare'])

    def test_seasons_do_not_mix_and_weekly_usage_preserves_unknown(self):
        features=[{'gsisId':'p1','season':2026}]
        current=pd.DataFrame([{'player_id':'p1','season':2026,'week':1,'team':'BUF','targets':4,'receiving_air_yards':-2}])
        snaps=pd.DataFrame([{'pfr_player_id':'pfr1','season':2025,'week':1,'offense_pct':1},
                            {'pfr_player_id':'pfr1','season':2026,'week':1,'offense_pct':.6}])
        out=enrich_opportunity(features,current,pd.DataFrame([{'gsis_id':'p1','pfr_id':'pfr1'}]),snaps,pd.DataFrame(),pd.DataFrame(),1)[0]['opportunity']
        self.assertEqual(out['snapShare'],.6)
        self.assertEqual(out['weeklyUsage'][0]['snapShare'],.6)
        self.assertEqual(out['weeklyUsage'][0]['receivingAirYards'],-2)
        self.assertIsNone(out['weeklyUsage'][0]['carries'])


if __name__ == '__main__':
    unittest.main()
