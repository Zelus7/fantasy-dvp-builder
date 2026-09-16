"""Observed workload evidence. Missing feeds remain null, never invented zeroes."""
from collections import defaultdict
import math


def number(value, default=0.0):
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def enrich_opportunity(features, current, players, snaps, pbp, depth, through):
    records = [r for r in current.to_dict('records')
               if 1 <= number(r.get('week')) <= through
               and str(r.get('season_type', 'REG')) == 'REG']
    totals = defaultdict(lambda: {'targets': 0, 'carries': 0})
    by_player = defaultdict(list)
    for row in records:
        team = row.get('recent_team') or row.get('team')
        key = (team, int(number(row.get('week'))))
        for stat in ('targets', 'carries'):
            totals[key][stat] += number(row.get(stat))
        by_player[str(row.get('player_id'))].append(row)
    pfr = {str(r.get('gsis_id')): str(r.get('pfr_id')) for r in players.to_dict('records')}
    snap_by_id = defaultdict(list)
    for row in snaps.to_dict('records'):
        if 1 <= number(row.get('week')) <= through and str(row.get('game_type', 'REG')) == 'REG':
            snap_by_id[str(row.get('pfr_player_id'))].append(row)
    redzone = defaultdict(int)
    pbp_teams = set()
    for row in pbp.to_dict('records'):
        week = int(number(row.get('week')))
        if not 1 <= week <= through or str(row.get('season_type', 'REG')) != 'REG':
            continue
        pbp_teams.add((row.get('posteam'), week))
        if number(row.get('no_play')) or number(row.get('yardline_100'), 101) > 20:
            continue
        if number(row.get('rush_attempt')) and not number(row.get('qb_kneel')):
            redzone[(str(row.get('rusher_player_id')), week)] += 1
        if number(row.get('pass_attempt')) and row.get('receiver_player_id'):
            redzone[(str(row.get('receiver_player_id')), week)] += 1
    depth_by_id = {}
    for row in sorted(depth.to_dict('records'), key=lambda r: str(r.get('dt') or r.get('timestamp') or '')):
        gsis = str(row.get('gsis_id') or row.get('player_id'))
        depth_by_id[gsis] = row
    for feature in features:
        gsis = feature['gsisId']
        rows = sorted(by_player.get(gsis, []), key=lambda r: number(r.get('week')))
        if not rows:
            feature['opportunity'] = None
            continue
        keys = {(r.get('recent_team') or r.get('team'), int(number(r.get('week')))) for r in rows}
        recent, earlier = rows[-3:], rows[:-3]
        avg = lambda data, key: sum(number(r.get(key)) for r in data) / len(data) if data else None
        denominator = lambda key: sum(totals[k][key] for k in keys)
        shares = {key: sum(number(r.get(key)) for r in rows) / denominator(key) if denominator(key) else None for key in ('targets', 'carries')}
        snap_rows = sorted(snap_by_id.get(pfr.get(gsis), []), key=lambda r: number(r.get('week')))
        snap = snap_rows[-1] if snap_rows else {}
        snap_pct = number(snap.get('offense_pct'), None)
        chart = depth_by_id.get(gsis, {})
        touchdowns = sum(number(r.get('rushing_tds')) + number(r.get('receiving_tds')) for r in recent)
        touches = sum(number(r.get('carries')) + number(r.get('receptions')) for r in recent)
        feature['targetShare'] = shares['targets']
        feature['opportunity'] = {
            'games': len(rows), 'throughWeek': max(int(number(r.get('week'))) for r in rows),
            'targetShare': shares['targets'], 'carryShare': shares['carries'],
            'recentTargets': avg(recent, 'targets'), 'recentGames': len(recent),
            'targetTrend': avg(recent, 'targets') - avg(earlier, 'targets') if earlier else None,
            'snapShare': snap_pct if snap_pct is not None and 0 <= snap_pct <= 1 else None,
            'snapWeek': int(number(snap.get('week'))) if snap else None,
            'redZoneOpportunities': sum(redzone[(gsis, week)] for _, week in keys) if keys <= pbp_teams else None,
            'depthPosition': str(chart.get('pos_rank') or chart.get('depth_position') or '') or None,
            'depthAsOf': str(chart.get('dt') or chart.get('timestamp') or '') or None,
            'touchdownDependent': touchdowns > 0 and touches / len(recent) < 6,
            'source': 'nflverse current-season player stats; PFR snaps and play-by-play when available',
            'weeklyPoints': feature.pop('_weeklyPoints', [])
        }
    return features
