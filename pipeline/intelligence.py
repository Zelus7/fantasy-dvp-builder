"""Public, source-dated context. These feeds do not silently retrain forecasts.

Only public SumerSports columns are accepted; locked fields are never used.
Absent reports mean unknown, not healthy. Name joins require team + position
and exactly one identity; no fuzzy player matching.
"""
import json
import math
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from html.parser import HTMLParser

import requests

ROUTES_URL = 'https://sumersports.com/players/wide-receiver/'
INJURIES_URL = 'https://site.api.espn.com/apis/site/v2/sports/football/nfl/injuries'
ALIASES = {'ARZ': 'ARI', 'AZ': 'ARI', 'BLT': 'BAL', 'CLV': 'CLE',
           'HST': 'HOU', 'JAC': 'JAX', 'LA': 'LAR', 'WAS': 'WSH'}
CLUBS = dict(zip(
    ['Cardinals','Falcons','Ravens','Bills','Panthers','Bears','Bengals','Browns',
     'Cowboys','Broncos','Lions','Packers','Texans','Colts','Jaguars','Chiefs',
     'Chargers','Rams','Raiders','Dolphins','Vikings','Patriots','Saints','Giants',
     'Jets','Eagles','Steelers','Seahawks','49ers','Buccaneers','Titans','Commanders'],
    ['ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE','DAL','DEN','DET','GB','HOU',
     'IND','JAX','KC','LAC','LAR','LV','MIA','MIN','NE','NO','NYG','NYJ','PHI',
     'PIT','SEA','SF','TB','TEN','WSH']))


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    except (TypeError, ValueError):
        return None


def name_key(value):
    value = unicodedata.normalize('NFKD', str(value or '')).encode('ascii', 'ignore').decode()
    value = re.sub(r'\s+(jr\.?|sr\.?|ii|iii|iv)$', '', value.strip(), flags=re.I)
    return re.sub(r'[^a-z0-9]', '', value.lower())


def club(value):
    value = str(value or '').strip().upper()
    return ALIASES.get(value, value)


def finite(value, low=0, high=100000):
    if isinstance(value, bool) or value is None:
        return None
    try:
        value = float(value)
        return round(value, 4) if math.isfinite(value) and low <= value <= high else None
    except (ValueError, TypeError):
        return None


def public_get(url):
    # Fixed public URLs only, no cookies/auth, bounded response and timeout.
    with requests.get(url, timeout=(10, 30), stream=True) as response:
        response.raise_for_status()
        data = bytearray()
        for chunk in response.iter_content(65536):
            data.extend(chunk)
            # ESPN includes full athlete objects (~9 MB today); retain a hard
            # cap while allowing the documented, league-wide public response.
            if len(data) > 16 * 1024 * 1024:
                raise ValueError('Public feed exceeded size limit')
        return data.decode('utf-8')


def parse_routes(html, season):
    # Next's public hydration payload supplies the same season and columns as
    # the rendered table. Parse JSON, never execute page scripts.
    chunks = []
    for encoded in re.findall(r'self\.__next_f\.push\((\[.*?\])\)</script>', html, re.S):
        try:
            value = json.loads(encoded)
            if len(value) == 2 and isinstance(value[1], str):
                chunks.append(value[1])
        except (ValueError, TypeError):
            continue
    body = ''.join(chunks)
    match = re.search(r'"initialData"\s*:', body)
    if not match:
        raise ValueError('Public receiving table unavailable')
    raw, _ = json.JSONDecoder().raw_decode(body[match.end():].lstrip())
    if not isinstance(raw, list) or len(raw) > 2000:
        raise ValueError('Unexpected receiving table')
    updated = re.search(r'<time\b[^>]*datetime="([^"]+)"', html, re.I)
    updated_at = timestamp(updated.group(1)) if updated else None
    if not updated_at:
        raise ValueError('Receiving source timestamp missing')
    rows = []
    for r in raw:
        p = r.get('sumerPlayer') or {}
        if r.get('season') != season or p.get('position') != 'WR':
            continue
        locked = set(r.get('lockedFields') or [])
        metric = lambda key, high: None if key in locked else finite(r.get(key), high=high)
        routes = metric('routesRun', 2000)
        if routes is None or not routes.is_integer():
            continue
        rows.append({'name': f"{p.get('footballName', '')} {p.get('lastName', '')}".strip(),
                     'team': club((r.get('sumerTeam') or {}).get('teamCode')),
                     'position': 'WR', 'season': season, 'routesRun': int(routes),
                     'targetsPerRouteRun': metric('targetsPerRouteRun', 1),
                     'yardsPerRouteRun': metric('yardsPerRouteRun', 100),
                     'averageDepthOfTarget': metric('adot', 100),
                     'sourceUpdatedAt': updated_at, 'sourceUrl': ROUTES_URL,
                     'source': 'SumerSports public WR table', 'sample': 'season-to-date',
                     'throughWeek': None, 'routeParticipation': None})
    if not rows:
        raise ValueError('No public routes for the requested season')
    return rows, updated_at


class PracticeTable(HTMLParser):
    def __init__(self):
        super().__init__()
        self.team = None
        self.in_table = False
        self.in_cell = False
        self.cells = []
        self.text = []
        self.rows = []
        self.headers = []

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.in_table = True
            self.headers = []
        if self.in_table and tag == 'tr':
            self.cells = []
        if self.in_table and tag in ('td', 'th'):
            self.in_cell = True
            self.text = []

    def handle_data(self, data):
        if not self.in_table and data.strip() in CLUBS:
            self.team = CLUBS[data.strip()]
        if self.in_cell:
            self.text.append(data)

    def handle_endtag(self, tag):
        if self.in_table and tag in ('td', 'th'):
            self.cells.append(' '.join(''.join(self.text).split()))
            self.in_cell = False
        if self.in_table and tag == 'tr':
            if self.cells == ['Player', 'Position', 'Injuries', 'Practice Status', 'Game Status']:
                self.headers = self.cells
            elif self.headers and len(self.cells) == 5 and self.team:
                self.rows.append((self.team, self.cells))
        if tag == 'table':
            self.in_table = False


def parse_practice(html, season, week):
    title = re.search(r'<title>(.*?)</title>', html, re.S | re.I)
    if not title or not re.search(rf'Week\s+{week}\b.*\b{season}\b', title.group(1)):
        raise ValueError('Practice report season/week mismatch')
    parser = PracticeTable()
    parser.feed(html)
    rows = []
    for team, cells in parser.rows:
        name, pos, injury, practice, status = cells
        if pos not in {'QB', 'RB', 'WR', 'TE'}:
            continue
        if practice and practice not in {'Did Not Participate In Practice',
                                        'Limited Participation in Practice',
                                        'Full Participation in Practice'}:
            continue
        rows.append({'name': name, 'team': team, 'position': pos, 'season': season,
                     'week': week, 'injury': injury or None, 'practiceStatus': practice or None,
                     'gameStatus': status or None, 'source': 'NFL official injury report',
                     'sourceUrl': f'https://www.nfl.com/injuries/league/{season}/reg{week}',
                     'reportDate': None})
    # Empty or unpublished reports do not clear existing injury designations.
    if not parser.rows:
        raise ValueError('Practice report not published')
    return rows, len({t for t, _ in parser.rows})


def parse_injuries(body, season):
    if (body.get('season') or {}).get('year') != season or (body.get('season') or {}).get('type') != 2:
        raise ValueError('Injury feed season mismatch')
    updated = timestamp(body.get('timestamp'))
    if not updated:
        raise ValueError('Injury source timestamp missing')
    rows = []
    for team in body.get('injuries', []):
        for r in team.get('injuries', []):
            athlete = r.get('athlete') or {}
            if (athlete.get('position') or {}).get('abbreviation') not in {'QB', 'RB', 'WR', 'TE'}:
                continue
            links = [l.get('href', '') for l in athlete.get('links', [])]
            source = next((l for l in links if re.match(r'https://www\.espn\.com/nfl/player/_/id/\d+/', l)), None)
            if not source:
                continue
            player_id = re.search(r'/id/(\d+)/', source).group(1)
            details = r.get('details') or {}
            rows.append({'espnId': player_id, 'season': season, 'status': str(r.get('status') or 'Unknown')[:50],
                         'reportedAt': timestamp(r.get('date')), 'sourceUpdatedAt': updated,
                         'bodyPart': str(details.get('type') or '')[:80] or None,
                         'headline': str(r.get('shortComment') or '')[:300],
                         'source': 'ESPN public injury report', 'sourceUrl': source,
                         'returnDate': None})
    if not isinstance(body.get('injuries'), list) or not body['injuries']:
        raise ValueError('Empty injury feed')
    return rows, updated


def load_intelligence(season, week, getter=public_get):
    result = {'routes': [], 'practice': [], 'injuries': [], 'coverage': {}}
    urls = {'routes': ROUTES_URL, 'practice': f'https://www.nfl.com/injuries/league/{season}/reg{week}',
            'injuries': INJURIES_URL}
    for key, url in urls.items():
        fetched = utc_now()
        try:
            raw = getter(url)
            extra = {}
            if key == 'routes':
                rows, updated = parse_routes(raw, season)
                extra = {'sourceUpdatedAt': updated, 'positions': ['WR'], 'sample': 'season-to-date'}
            elif key == 'practice':
                rows, teams = parse_practice(raw, season, week)
                extra = {'week': week, 'teamsWithReports': teams, 'reportDate': None}
            else:
                rows, updated = parse_injuries(json.loads(raw), season)
                extra = {'sourceUpdatedAt': updated}
            result[key] = [{**r, 'fetchedAt': fetched} for r in rows]
            result['coverage'][key] = {'status': 'available', 'rows': len(rows),
                                       'fetchedAt': fetched, 'season': season, 'sourceUrl': url, **extra}
        except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError) as error:
            result['coverage'][key] = {'status': 'unavailable', 'fetchedAt': fetched,
                                       'season': season, 'sourceUrl': url, 'errorType': type(error).__name__}
    return result


def attach_intelligence(features, data):
    identities = defaultdict(list)
    for f in features:
        identities[(name_key(f['playerName']), club(f.get('team')), f['position'])].append(f)
    by_id = {str(f['espnId']): f for f in features}
    matched = defaultdict(int)
    for kind in ('routes', 'practice', 'injuries'):
        grouped = defaultdict(list)
        for row in data.get(kind, []):
            if kind == 'injuries':
                f = by_id.get(row['espnId'])
            else:
                matches = identities.get((name_key(row['name']), row['team'], row['position']), [])
                f = matches[0] if len(matches) == 1 else None
            if f and f['season'] == row['season']:
                grouped[str(f['espnId'])].append(row)
        for player_id, rows in grouped.items():
            # Conflicting duplicates are unavailable, never silently selected.
            unique = {json.dumps(r, sort_keys=True): r for r in rows}
            if len(unique) != 1:
                continue
            f = by_id[player_id]
            f['opportunity'] = f.get('opportunity') or {'games': 0, 'throughWeek': 0}
            f['opportunity'][kind] = next(iter(unique.values()))
            matched[kind] += 1
        if kind in data['coverage']:
            data['coverage'][kind]['matchedPlayers'] = matched[kind]
    return features
