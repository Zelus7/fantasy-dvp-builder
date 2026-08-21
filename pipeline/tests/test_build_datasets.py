import unittest
from unittest.mock import patch
import pandas as pd

from pipeline.build_datasets import dvp_rows, feature_rows, grade, load_player_stats_with_preseason_fallback, prior_weight, schedule_rows, score_row

SCORING=[{'statId':3,'points':.04},{'statId':4,'points':4},{'statId':20,'points':-2},{'statId':24,'points':.1},{'statId':25,'points':6},{'statId':42,'points':.1},{'statId':43,'points':6},{'statId':53,'points':.5},{'statId':72,'points':-2}]
PLAYERS=pd.DataFrame([
 {'gsis_id':'p1','espn_id':'101','display_name':'Receiver','position':'WR','latest_team':'MIA'},
 {'gsis_id':'p2','espn_id':'102','display_name':'Runner','position':'RB','latest_team':'BUF'},
])

class PipelineTests(unittest.TestCase):
 def test_common_half_ppr_scoring(self):
  points,unsupported=score_row({'receptions':6,'receiving_yards':80,'receiving_tds':1,'fumbles_lost':1},SCORING)
  self.assertAlmostEqual(points,15.0)
  self.assertEqual(unsupported,set())

 def test_unknown_scoring_is_reported(self):
  _,unsupported=score_row({'receiving_yards':10},[{'statId':999,'points':2}])
  self.assertEqual(unsupported,{999})

 def test_prior_weight_tapers_to_zero(self):
  self.assertEqual(prior_weight(0),1.0)
  self.assertEqual(prior_weight(3),.45)
  self.assertEqual(prior_weight(6),0.0)

 def test_grade_direction_matches_points_allowed(self):
  self.assertEqual(grade(95),'A')
  self.assertEqual(grade(5),'F')

 def test_dvp_is_per_game_and_ranked_most_allowed_first(self):
  current=pd.DataFrame([
   {'season':2026,'week':1,'player_id':'p1','position':'WR','recent_team':'MIA','opponent_team':'BUF','receptions':8,'receiving_yards':120,'receiving_tds':1},
   {'season':2026,'week':1,'player_id':'p1','position':'WR','recent_team':'MIA','opponent_team':'NYJ','receptions':2,'receiving_yards':20,'receiving_tds':0},
  ])
  prior=pd.DataFrame()
  rows,_=dvp_rows(current,prior,PLAYERS,SCORING,2026,1,['BUF','NYJ'])
  season=[row for row in rows if row['window']=='season' and row['position']=='WR']
  self.assertEqual(season[0]['defenseTeam'],'BUF')
  self.assertEqual(season[0]['rank'],1)
  self.assertGreater(season[0]['pointsAllowedPerGame'],season[1]['pointsAllowedPerGame'])

 def test_player_features_use_espn_identity_and_touches(self):
  current=pd.DataFrame([
   {'season':2026,'week':1,'player_id':'p2','position':'RB','recent_team':'BUF','opponent_team':'MIA','carries':15,'rushing_yards':75,'rushing_tds':1,'receptions':3,'receiving_yards':20,'targets':4},
  ])
  rows,_=feature_rows(current,pd.DataFrame(),PLAYERS,SCORING,2026,1)
  runner=next(row for row in rows if row['espnId']=='102')
  self.assertEqual(runner['touchesPerGame'],18)
  self.assertGreater(runner['seasonPpg'],0)

 def test_schedule_normalizes_team_aliases(self):
  frame=pd.DataFrame([{'season':2026,'week':1,'game_id':'g1','gameday':'2026-09-10','gametime':'20:20','home_team':'WAS','away_team':'JAC','stadium':'Example','roof':'outdoors'}])
  rows=schedule_rows(frame,2026)
  self.assertEqual(rows[0]['homeTeam'],'WSH')
  self.assertEqual(rows[0]['awayTeam'],'JAX')
  self.assertTrue(rows[0]['kickoff'].endswith('Z'))

 def test_missing_current_player_stats_uses_prior_season(self):
  prior=pd.DataFrame([{'season':2025,'week':1}])
  def load(_name,seasons):
   if seasons==[2026]: raise ConnectionError('404 Client Error: stats_player_week_2026.parquet')
   return prior
  with patch('pipeline.build_datasets.load_nflreadpy',side_effect=load):
   result=load_player_stats_with_preseason_fallback([2026])
  self.assertEqual(result['season'].tolist(),[2025])

 def test_current_player_stats_network_failure_does_not_fall_back(self):
  def load(_name,seasons):
   if seasons==[2026]: raise ConnectionError('connection timed out')
   return pd.DataFrame([{'season':2025,'week':1}])
  with patch('pipeline.build_datasets.load_nflreadpy',side_effect=load):
   with self.assertRaisesRegex(ConnectionError,'timed out'):
    load_player_stats_with_preseason_fallback([2026])

if __name__=='__main__': unittest.main()
