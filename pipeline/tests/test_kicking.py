import unittest
import pandas as pd
from pipeline.kicking import kicking_features


class KickingTests(unittest.TestCase):
    def setUp(self):
        self.players = pd.DataFrame([dict(gsis_id='k1', espn_id='123.0', position='K', display_name='Kicker')])
        self.schedule = [dict(eventId=f'g{w}', season=2026, week=w, homeTeam='IND', awayTeam='PIT', status='STATUS_FINAL') for w in range(1, 5)]
        self.plays = [dict(game_id=f'g{w}', play_id=i, season=2026, season_type='REG', week=w, posteam='IND',
                           kicker_player_id='k1', field_goal_attempt=1, field_goal_result='made',
                           extra_point_attempt=0, extra_point_result=None, no_play=0, kick_distance=52)
                      for w, count in enumerate([1, 3, 4, 3], 1) for i in range(count)]

    def build(self, plays=None, weekly=None):
        return kicking_features(pd.DataFrame(self.plays if plays is None else plays), self.players,
                                pd.DataFrame(weekly or []), self.schedule, 2026, 4, '2026-10-07T12:00:00Z')

    def test_attempt_volume_is_observed_not_fantasy_points(self):
        rows, coverage = self.build()
        k = rows[0]['opportunity']['kicking']
        self.assertEqual(k['attempts'], 11)
        self.assertEqual(k['longAttempts'], 11)
        self.assertEqual(k['games'], 4)
        self.assertTrue(k['coverageComplete'])
        self.assertEqual(rows[0]['espnId'], '123')
        self.assertTrue(rows[0]['opportunity']['contextOnly'])
        self.assertEqual(rows[0]['games'], 0)
        self.assertEqual(coverage['players'], 1)

    def test_excludes_wrong_season_future_unfinished_nullified_and_duplicate_plays(self):
        base = self.plays[0]
        extras = [dict(base, season=2025), dict(base, game_id='future', week=5), dict(base, no_play=1, play_id=50), dict(base)]
        rows, _ = self.build(self.plays + extras)
        self.assertEqual(rows[0]['opportunity']['kicking']['attempts'], 11)
        self.schedule[3]['status'] = 'STATUS_SCHEDULED'
        rows, _ = self.build()
        self.assertEqual(rows[0]['opportunity']['kicking']['attempts'], 8)

    def test_missing_distance_is_unknown_and_blocked_attempt_still_counts(self):
        plays = [dict(self.plays[0], kick_distance=None, field_goal_result='blocked')]
        rows, _ = self.build(plays)
        k = rows[0]['opportunity']['kicking']
        self.assertEqual((k['attempts'], k['made'], k['unknownDistances']), (1, 0, 1))
        self.assertFalse(k['coverageComplete'])

    def test_real_zero_requires_weekly_participation_not_assumed_schedule(self):
        plays = [p for p in self.plays if p['week'] != 4]
        rows, _ = self.build(plays)
        self.assertFalse(rows[0]['opportunity']['kicking']['coverageComplete'])
        rows, _ = self.build(plays, [dict(player_id='k1', position='K', recent_team='IND', week=4, season=2026, fg_att=0, pat_att=0)])
        k = rows[0]['opportunity']['kicking']
        self.assertTrue(k['coverageComplete'])
        self.assertEqual(k['weekly'][-1]['attempts'], 0)

    def test_missing_feed_or_identity_cannot_fabricate_player(self):
        rows, coverage = self.build([])
        self.assertEqual(rows, [])
        self.assertEqual(coverage['status'], 'missing')
        self.players.loc[0, 'espn_id'] = None
        rows, _ = self.build()
        self.assertEqual(rows, [])

    def test_nflverse_play_type_no_play_schema(self):
        plays = [dict(p, play_type='field_goal') for p in self.plays]
        for p in plays:
            del p['no_play']
        plays.append(dict(plays[0], play_id=99, play_type='no_play'))
        rows, _ = self.build(plays)
        self.assertEqual(rows[0]['opportunity']['kicking']['attempts'], 11)

    def test_pat_only_game_is_counted_without_fabricating_field_goal_attempts(self):
        rows, _ = self.build([dict(self.plays[0], field_goal_attempt=0, extra_point_attempt=1, extra_point_result='good')])
        k = rows[0]['opportunity']['kicking']
        self.assertEqual((k['games'], k['attempts'], k['patMade']), (1, 0, 1))

    def test_unknown_result_is_not_counted_as_a_miss_or_silently_omitted(self):
        rows, coverage = self.build(self.plays + [dict(self.plays[0], play_id=99, field_goal_result=None)])
        self.assertEqual(rows, [])
        self.assertEqual(coverage['status'], 'missing')


if __name__ == '__main__':
    unittest.main()
