import copy
import json
import unittest
from unittest.mock import patch, MagicMock
from pipeline.intelligence import (parse_routes, parse_practice, parse_injuries,
                                   load_intelligence, attach_intelligence, public_get)


def routes_html(rows):
    payload = '1:' + json.dumps({'initialData': rows})
    return '<time datetime="2026-09-29T16:56:30Z"></time><script>self.__next_f.push(' + json.dumps([1, payload]) + ')</script>'


def route_row(**extra):
    return {'season': 2026, 'sumerPlayer': {'footballName': 'Jakobi', 'lastName': 'Meyers', 'position': 'WR'},
            'sumerTeam': {'teamCode': 'JAX'}, 'routesRun': 78, 'targetsPerRouteRun': 11 / 78,
            'yardsPerRouteRun': 132 / 78, 'adot': 10.36, **extra}


PRACTICE = '''<title>Official NFL Injury Report for Players - Week 4 of the 2026 Season | NFL.com</title>
<div><span>Browns</span></div><table><thead><tr><th>Player</th><th>Position</th><th>Injuries</th><th>Practice Status</th><th>Game Status</th></tr></thead>
<tbody><tr><td><a>Denzel Boston</a></td><td>WR</td><td>Back</td><td>Limited Participation in Practice</td><td></td></tr></tbody></table>'''


class IntelligenceTests(unittest.TestCase):
    def test_routes_use_public_values_not_invented_participation(self):
        rows, updated = parse_routes(routes_html([route_row()]), 2026)
        self.assertEqual(rows[0]['routesRun'], 78)
        self.assertAlmostEqual(rows[0]['targetsPerRouteRun'], .141, places=3)
        self.assertIsNone(rows[0]['routeParticipation'])
        self.assertIsNone(rows[0]['throughWeek'])
        self.assertEqual(rows[0]['sample'], 'season-to-date')

    def test_locked_fields_are_not_read(self):
        rows, _ = parse_routes(routes_html([route_row(lockedFields=['yardsPerRouteRun'])]), 2026)
        self.assertIsNone(rows[0]['yardsPerRouteRun'])
        with self.assertRaises(ValueError):
            parse_routes(routes_html([route_row(lockedFields=['routesRun'])]), 2026)

    def test_wrong_season_missing_date_and_invalid_numbers_fail_closed(self):
        for html in [routes_html([route_row(season=2025)]),
                     routes_html([route_row()]).replace('datetime=', 'unknown='),
                     routes_html([route_row(routesRun=-3)]),
                     routes_html([route_row(routesRun=3.2)])]:
            with self.assertRaises(ValueError):
                parse_routes(html, 2026)

    def test_official_practice_is_exact_week_not_a_daily_trend(self):
        rows, teams = parse_practice(PRACTICE, 2026, 4)
        self.assertEqual(teams, 1)
        self.assertEqual(rows[0]['team'], 'CLE')
        self.assertEqual(rows[0]['practiceStatus'], 'Limited Participation in Practice')
        self.assertIsNone(rows[0]['reportDate'])
        self.assertIsNone(rows[0]['gameStatus'])
        with self.assertRaises(ValueError):
            parse_practice(PRACTICE, 2026, 5)
        with self.assertRaises(ValueError):
            parse_practice(PRACTICE, 2025, 4)

    def test_injury_ids_come_from_public_player_links_and_no_guessed_return(self):
        body = {'season': {'year': 2026, 'type': 2}, 'timestamp': '2026-09-30T10:00Z',
                'injuries': [{'injuries': [{'date': '2026-09-28T10:00Z', 'status': 'Out',
                                          'details': {'returnDate': '2026-10-01'},
                                          'athlete': {'position': {'abbreviation': 'RB'}, 'links': [
                                              {'href': 'https://www.espn.com/nfl/player/_/id/123/test'}]}}]}]}
        rows, _ = parse_injuries(body, 2026)
        self.assertEqual(rows[0]['espnId'], '123')
        self.assertIsNone(rows[0]['returnDate'])
        self.assertEqual(rows[0]['reportedAt'], '2026-09-28T10:00:00Z')
        with self.assertRaises(ValueError):
            parse_injuries(body, 2025)

    def test_optional_feed_failures_do_not_claim_coverage_or_block_core_data(self):
        result = load_intelligence(2026, 4, getter=lambda _: '{}')
        self.assertTrue(all(v['status'] == 'unavailable' for v in result['coverage'].values()))
        self.assertEqual(result['routes'], [])
        malformed = load_intelligence(2026, 4, getter=lambda _: '[null]')
        self.assertTrue(all(v['status'] == 'unavailable' for v in malformed['coverage'].values()))

    def test_public_response_limit_allows_current_espn_feed_but_stays_bounded(self):
        with patch('pipeline.intelligence.requests.get') as get:
            response = MagicMock()
            get.return_value.__enter__.return_value = response
            response.iter_content.return_value = [b'x' * 1024 * 1024] * 9
            self.assertEqual(len(public_get('https://example.test')), 9 * 1024 * 1024)
            response.iter_content.return_value = [b'x' * 1024 * 1024] * 17
            with self.assertRaisesRegex(ValueError, 'size limit'):
                public_get('https://example.test')

    def test_ambiguous_player_and_conflicting_rows_are_not_joined(self):
        feature = {'espnId': '1', 'season': 2026, 'playerName': 'Jakobi Meyers', 'team': 'JAX', 'position': 'WR'}
        rows, _ = parse_routes(routes_html([route_row()]), 2026)
        data = {'routes': rows, 'coverage': {'routes': {}}}
        f = copy.deepcopy(feature)
        attach_intelligence([f], data)
        self.assertEqual(f['opportunity']['routes']['routesRun'], 78)
        f = copy.deepcopy(feature)
        attach_intelligence([f, {**feature, 'espnId': '2'}], data)
        self.assertNotIn('opportunity', f)
        f = copy.deepcopy(feature)
        attach_intelligence([f], {'routes': rows + [{**rows[0], 'routesRun': 79}], 'coverage': {}})
        self.assertNotIn('opportunity', f)
        f = {**feature, 'team': 'IND'}
        attach_intelligence([f], data)
        self.assertNotIn('opportunity', f)


if __name__ == '__main__':
    unittest.main()
