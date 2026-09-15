import copy
import unittest
from unittest.mock import patch

from pipeline.build_datasets import POSITIONS, TEAMS, WINDOWS, publish_datasets, validate_publication


def complete_payloads():
    clubs=sorted(TEAMS)
    games=[{'eventId':f'{week}-{i}', 'season':2026, 'week':week,
            'homeTeam':clubs[i], 'awayTeam':clubs[i+16]}
           for week in range(1,18) for i in range(16)]
    dvp=[{'defenseTeam':club, 'position':pos, 'window':window,
          'pointsAllowedPerGame':10.0, 'percentile':50.0}
         for club in clubs for pos in POSITIONS for window in WINDOWS]
    features=[{'espnId':str(i+1), 'seasonPpg':10.0} for i in range(100)]
    return [(f'/api/internal/pipeline/{kind}',
             {'metadata':{'season':2026,'throughWeek':1},'rows':rows})
            for kind,rows in [('schedule',games),('dvp',dvp),('player-features',features)]]


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.payloads=complete_payloads()

    def test_valid_complete_batch(self):
        validate_publication(self.payloads)

    def test_empty_batch(self):
        with self.assertRaisesRegex(ValueError,'No datasets'):
            validate_publication([])

    def test_incomplete_schedule(self):
        self.payloads[0][1]['rows'].pop()
        with self.assertRaisesRegex(ValueError,'Incomplete regular-season'):
            validate_publication(self.payloads)

    def test_duplicate_team_week(self):
        rows=self.payloads[0][1]['rows']
        rows[1]['homeTeam']=rows[0]['homeTeam']
        with self.assertRaisesRegex(ValueError,'duplicate schedule'):
            validate_publication(self.payloads)

    def test_duplicate_dvp_row_cannot_replace_missing_coverage(self):
        rows=self.payloads[1][1]['rows']
        rows[1]=copy.deepcopy(rows[0])
        with self.assertRaisesRegex(ValueError,'DvP coverage'):
            validate_publication(self.payloads)

    def test_small_feature_feed(self):
        self.payloads[2][1]['rows'].pop()
        with self.assertRaisesRegex(ValueError,'feature coverage'):
            validate_publication(self.payloads)

    def test_duplicate_player_identity(self):
        rows=self.payloads[2][1]['rows']
        rows[1]['espnId']=rows[0]['espnId']
        with self.assertRaisesRegex(ValueError,'feature coverage'):
            validate_publication(self.payloads)

    def test_nonfinite_nested_data(self):
        self.payloads[2][1]['rows'][0]['recentPpg']=float('nan')
        with self.assertRaises(ValueError):
            validate_publication(self.payloads)

    def test_bad_final_dataset_prevents_every_upload(self):
        self.payloads[2][1]['rows']=[]
        with patch('pipeline.build_datasets.upload') as upload:
            with self.assertRaises(ValueError):
                publish_datasets('https://example.invalid','unused',self.payloads)
            upload.assert_not_called()


if __name__=='__main__':
    unittest.main()
