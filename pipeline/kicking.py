"""Observed kicking opportunity, not a fantasy-points forecast.

Only identified kickers and completed, regular-season games enter the sample.
Zero-attempt games are retained only when a unique kicker is identified in that
game's weekly player statistics. Missing participation is never invented.
"""
from collections import defaultdict
try:
    from .opportunity import number
except ImportError:
    from opportunity import number


def kicking_features(pbp, players, weekly, schedules, season, through, generated_at):
    identities = {}
    for p in players.to_dict('records'):
        gid, eid = str(p.get('gsis_id') or ''), str(p.get('espn_id') or '')
        if eid.endswith('.0'):
            eid = eid[:-2]
        if gid and eid.isdigit() and str(p.get('position')).upper() == 'K':
            identities[gid] = (eid, p)
    completed = {str(g['eventId']) for g in schedules if g['season'] == season
                 and g['week'] <= through and g['status'] == 'STATUS_FINAL'}
    columns = set(pbp.columns)
    required = {'game_id', 'season', 'season_type', 'week', 'posteam',
                'kicker_player_id', 'field_goal_attempt', 'field_goal_result',
                'extra_point_attempt', 'extra_point_result'}
    if not required <= columns or not ({'no_play', 'play_type'} & columns) or not completed:
        return [], {'status': 'missing', 'players': 0, 'throughWeek': through,
                    'reason': 'Completed-game play-by-play or kicking columns unavailable.'}
    games, seen, invalid = {}, set(), set()
    def game(gid, club, week, event):
        key = (gid, club, week, event)
        if key not in games:
            games[key] = dict(week=week, team=club, eventId=event, attempts=0,
                              made=0, patAttempts=0, patMade=0, longAttempts=0,
                              longMade=0, unknownDistances=0)
        return games[key]
    for r in pbp.to_dict('records'):
        event = str(r.get('game_id'))
        if event not in completed or number(r.get('season')) != season or r.get('season_type') != 'REG' or number(r.get('no_play')) or r.get('play_type') == 'no_play':
            continue
        club = {'LA': 'LAR', 'JAC': 'JAX', 'WAS': 'WSH', 'OAK': 'LV', 'SD': 'LAC'}.get(r.get('posteam'), r.get('posteam'))
        gid, week = str(r.get('kicker_player_id')), int(number(r.get('week')))
        key = (event, r.get('play_id'))
        if 'play_id' in columns and key in seen:
            continue
        seen.add(key)
        fg, pat = number(r.get('field_goal_attempt')) == 1, number(r.get('extra_point_attempt')) == 1
        if gid not in identities or not club or not (fg or pat):
            continue
        # An absent result is unknown, not a missed kick. Refuse this player's
        # feed rather than expose apparently perfect/poor accuracy from gaps.
        if (fg and r.get('field_goal_result') not in ('made', 'missed', 'blocked')) or (pat and r.get('extra_point_result') not in ('good', 'failed', 'blocked')):
            invalid.add(gid)
            continue
        row = game(gid, club, week, event)
        if fg:
            row['attempts'] += 1
            made = r.get('field_goal_result') == 'made'
            row['made'] += int(made)
            distance = number(r.get('kick_distance'), None)
            row['unknownDistances'] += int(distance is None)
            row['longAttempts'] += int(distance is not None and distance >= 50)
            row['longMade'] += int(made and distance is not None and distance >= 50)
        if pat:
            row['patAttempts'] += 1
            row['patMade'] += int(r.get('extra_point_result') == 'good')
    # Weekly kicking rows can establish participation with zero FG/PAT attempts.
    game_lookup = {(g['week'], club): g['eventId'] for g in schedules
                   if g['eventId'] in completed for club in (g['homeTeam'], g['awayTeam'])}
    for r in weekly.to_dict('records'):
        gid = str(r.get('player_id') or '')
        club, week = r.get('recent_team') or r.get('team'), int(number(r.get('week')))
        event = game_lookup.get((week, club))
        if (gid in identities and event and number(r.get('season')) == season
                and r.get('season_type', 'REG') == 'REG' and r.get('position') == 'K'
                and 'fg_att' in r and number(r.get('fg_att'), None) == 0
                and 'pat_att' in r and number(r.get('pat_att'), None) == 0):
            game(gid, club, week, event)
    grouped = defaultdict(list)
    for (gid, _, _, _), row in games.items():
        grouped[gid].append(row)
    output = []
    for gid, rows in grouped.items():
        if gid in invalid:
            continue
        rows.sort(key=lambda r: (r['week'], r['eventId']))
        eid, p = identities[gid]
        current_team = rows[-1]['team']
        rows = [r for r in rows if r['team'] == current_team]
        expected = sum(1 for g in schedules if g['eventId'] in completed and current_team in (g['homeTeam'], g['awayTeam']))
        kicking = dict(season=season, team=current_team, games=len(rows),
                       throughWeek=max(r['week'] for r in rows), datasetThroughWeek=through, generatedAt=generated_at,
                       source='nflverse completed-game play-by-play',
                       sourceUrl='https://github.com/nflverse/nflverse-data/releases/tag/pbp',
                       weekly=rows, teamGames=expected, coverageComplete=len(rows)==expected,
                       sample='Observed games with identified kicker participation; not an assumed full schedule.',
                       **{k: sum(r[k] for r in rows) for k in ('attempts', 'made', 'patAttempts', 'patMade', 'longAttempts', 'longMade', 'unknownDistances')})
        # DB's legacy numeric columns are NOT kicking fantasy scores. The reader
        # explicitly restores nulls for this context-only record.
        output.append(dict(espnId=eid, gsisId=gid, playerName=p.get('display_name') or p.get('full_name') or gid,
                           position='K', team=current_team, season=season, games=0,
                           currentGames=0, seasonPpg=0, opportunity={'contextOnly': True, 'kicking': kicking}))
    return output, {'status': 'available' if output else 'missing', 'players': len(output),
                    'throughWeek': through, 'generatedAt': generated_at,
                    'coverage': 'Completed-game kicking opportunities; no independently scored fantasy averages or calibrated kicker forecast.'}
