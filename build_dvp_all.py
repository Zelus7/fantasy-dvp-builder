import argparse
import collections
import datetime
import json
import sys
import time

import requests


# ---------- Config knobs ----------
SLEEP_BETWEEN_CALLS = 0.05  # seconds between Sleeper requests to be nice
DEFAULT_POSITIONS = ["WR", "RB", "QB", "TE"]
SLEEPER_PLAYERS_URL = "https://api.sleeper.com/players/nfl"
SLEEPER_PLAYER_WEEKLY_URL = (
    "https://api.sleeper.com/stats/nfl/player/{pid}"
    "?season={season}&season_type=regular&week={week}"
)


# ---------- helpers ----------

def log(*args, **kwargs):
    print(*args, **kwargs, flush=True)


def get_current_season():
    return datetime.datetime.now().year


def get_current_week_from_worker(worker_url, season, league_id, s2, swid):
    """
    Call your Worker /currentweek endpoint and try to derive currentWeek.
    Assumes the Worker forwards to ESPN scoreboard. We handle both:
      - a trimmed { "currentWeek": N } style response, OR
      - raw scoreboard with status.currentMatchupPeriod / scoringPeriodId.
    """
    url = f"{worker_url.rstrip('/')}/currentweek"
    headers = {
        "X-ESPN-S2": s2,
        "X-ESPN-SWID": swid,
        "Accept": "application/json",
    }
    params = {"season": str(season), "leagueId": str(league_id)}

    log(f"[currentweek] GET {url} season={season} leagueId={league_id}")
    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    # 1) Ideal shape
    wk = data.get("currentWeek")
    if wk:
        return int(wk)

    # 2) Scoreboard-like shape
    status = data.get("status") or {}
    wk = status.get("currentMatchupPeriod") or status.get("latestScoringPeriod")
    if wk:
        return int(wk)

    wk = data.get("scoringPeriodId")
    if wk:
        return int(wk)

    raise RuntimeError("Could not determine currentWeek from /currentweek response")


def fetch_sleeper_players():
    log("[players] downloading Sleeper players...")
    resp = requests.get(SLEEPER_PLAYERS_URL, timeout=60)
    resp.raise_for_status()
    players = resp.json()
    if not isinstance(players, dict):
        raise RuntimeError("Unexpected Sleeper /players/nfl response shape")
    log(f"[players] loaded {len(players)} players")
    return players


def filter_player_ids_by_pos(players, pos):
    pos = pos.upper()
    ids = []
    for pid, p in players.items():
        fp = p.get("fantasy_positions") or []
        if isinstance(fp, list) and pos in fp:
            ids.append(str(pid))
    log(f"[pos={pos}] candidate players: {len(ids)}")
    return ids


def compute_points(stats, scoring):
    """
    Compute fantasy points using:
      1) direct Sleeper point fields if present
      2) otherwise from yards/TDs
    scoring: "half" | "ppr" | "std"
    """
    def N(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    # 1) direct point fields if they exist
    point_keys = (
        "pts_half_ppr",
        "fpts_half_ppr",
        "pts_hppr",
        "pts_ppr",
        "pts_std",
        "fpts",
    )
    for k in point_keys:
        v = stats.get(k)
        if isinstance(v, (int, float)):
            return float(v)

    # 2) from components
    rec = stats.get("rec") or stats.get("receptions") or 0
    rec_yd = (
        stats.get("rec_yd")
        or stats.get("receiving_yards")
        or stats.get("receiving_yds")
        or 0
    )
    rec_td = stats.get("rec_td") or stats.get("receiving_tds") or 0

    rush_yd = (
        stats.get("rush_yd")
        or stats.get("rushing_yards")
        or stats.get("rushing_yds")
        or 0
    )
    rush_td = stats.get("rush_td") or stats.get("rushing_tds") or 0

    pass_yd = (
        stats.get("pass_yd")
        or stats.get("passing_yards")
        or stats.get("passing_yds")
        or 0
    )
    pass_td = stats.get("pass_td") or stats.get("passing_tds") or 0

    ints = stats.get("interceptions") or stats.get("ints") or 0
    fum = stats.get("fum_lost") or stats.get("fumbles_lost") or stats.get("fum") or 0

    rec = N(rec)
    rec_yd = N(rec_yd)
    rec_td = N(rec_td)
    rush_yd = N(rush_yd)
    rush_td = N(rush_td)
    pass_yd = N(pass_yd)
    pass_td = N(pass_td)
    ints = N(ints)
    fum = N(fum)

    if scoring == "ppr":
        base = 1.0 * rec
    elif scoring == "half":
        base = 0.5 * rec
    else:
        base = 0.0  # std

    pts = (
        base
        + rec_yd / 10.0
        + 6 * rec_td
        + rush_yd / 10.0
        + 6 * rush_td
        + pass_yd / 25.0
        + 4 * pass_td
        - 2 * ints
        - 2 * fum
    )
    return float(pts)


def build_dvp_for_pos(pos, season, weeks, scoring, player_ids, max_players=None):
    """
    For a given position (WR/RB/QB/TE), aggregate fantasy points allowed
    by defense from Sleeper per-player weekly stats.
    Returns: (totals_by_defense, stats_dict)
    """
    pos = pos.upper()
    if max_players:
        ids = player_ids[:max_players]
    else:
        ids = player_ids

    totals = collections.defaultdict(float)
    calls_ok = calls_err = calls_null = 0
    rows_pts = rows_opp = 0

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }

    for pid in ids:
        for w in weeks:
            url = SLEEPER_PLAYER_WEEKLY_URL.format(pid=pid, season=season, week=w)
            try:
                resp = requests.get(url, headers=headers, timeout=12)
            except Exception:
                calls_err += 1
                continue

            if resp.status_code != 200:
                calls_err += 1
                continue

            calls_ok += 1
            text = resp.text.strip()
            if not text or text == "null":
                calls_null += 1
                continue

            try:
                row = resp.json()
            except Exception:
                calls_err += 1
                continue

            if not isinstance(row, dict):
                continue

            stats = (
                row.get("stats")
                if isinstance(row.get("stats"), dict)
                else row  # some shapes are flat
            )
            if not isinstance(stats, dict):
                continue

            opp = (
                row.get("opponent")
                or row.get("opp")
                or row.get("opponent_team")
                or row.get("opp_abbr")
                or ""
            )
            opp = str(opp).upper()
            if not opp:
                continue

            pts = compute_points(stats, scoring)
            if pts <= 0:
                continue

            rows_pts += 1
            rows_opp += 1
            totals[opp] += pts

            time.sleep(SLEEP_BETWEEN_CALLS)

    info = {
        "calls_ok": calls_ok,
        "calls_err": calls_err,
        "calls_null": calls_null,
        "rows_with_pts": rows_pts,
        "rows_with_opp": rows_opp,
        "players_used": len(ids),
    }
    return totals, info


def make_dvp_doc(pos, season, weeks, scoring, totals, source_tag):
    items = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)
    data = [
        {"rank": i + 1, "team": team, "pointsAllowed": round(pts, 1)}
        for i, (team, pts) in enumerate(items)
    ]
    return {
        "position": pos,
        "season": season,
        "weeks": weeks,
        "through": max(weeks) if weeks else None,
        "scoring": scoring,
        "source": source_tag,
        "generatedAt": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "data": data,
    }


def upload_to_worker(worker_url, doc):
    url = f"{worker_url.rstrip('/')}/dvp/cache_put"
    resp = requests.post(url, json=doc, timeout=60)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text[:200]}
    if not resp.ok:
        raise RuntimeError(f"cache_put failed {resp.status}: {body}")
    return body


def main():
    parser = argparse.ArgumentParser(description="Build DvP for multiple positions and upload to Worker KV.")
    parser.add_argument("--worker-url", required=True, help="Base URL of your Worker, e.g. https://espn-fantasy-proxy.acarvall87.workers.dev")
    parser.add_argument("--leagueId", required=True, help="ESPN leagueId (used to find currentWeek)")
    parser.add_argument("--s2", required=True, help="espn_s2 cookie value")
    parser.add_argument("--swid", required=True, help="SWID cookie value (with or without braces)")
    parser.add_argument("--scoring", choices=["std", "half", "ppr"], default="half", help="Scoring model (std/half/ppr)")
    parser.add_argument("--season", type=int, default=None, help="Override season year (defaults to current year)")
    parser.add_argument("--through", type=int, default=None, help="Override through week (defaults to current fantasy week)")
    parser.add_argument("--positions", default="WR,RB,QB,TE", help="Comma-separated positions to build (default WR,RB,QB,TE)")
    parser.add_argument("--max-players-per-pos", type=int, default=None, help="Optional cap on players per position (for testing / rate limiting)")

    args = parser.parse_args()

    worker_url = args.worker_url
    league_id = args.leagueId
    s2 = args.s2
    swid = args.swid
    scoring = args.scoring
    season = args.season or get_current_season()
    positions = [p.strip().upper() for p in args.positions.split(",") if p.strip()]
    max_players = args.max_players_per_pos

    # Normalize SWID (Worker expects braces)
    if not swid.startswith("{"):
        swid = "{" + swid.strip("{}") + "}"

    # 1) Determine currentWeek if not provided
    if args.through is not None:
        through_week = int(args.through)
        log(f"[week] using provided through week: {through_week}")
    else:
        through_week = get_current_week_from_worker(worker_url, season, league_id, s2, swid)
        log(f"[week] current fantasy week from Worker/ESPN: {through_week}")

    weeks = list(range(1, through_week + 1))
    log(f"[weeks] aggregating weeks {weeks[0]}–{weeks[-1]} (inclusive)")

    # 2) Load players once
    players = fetch_sleeper_players()

    # 3) Loop positions
    for pos in positions:
        log(f"\n=== Building DvP for pos={pos} season={season} scoring={scoring} ===")
        pids = filter_player_ids_by_pos(players, pos)
        totals, info = build_dvp_for_pos(pos, season, weeks, scoring, pids, max_players=max_players)
        log(f"[{pos}] stats: {info}")
        if not totals:
            log(f"[{pos}] WARNING: no totals computed, skipping upload.")
            continue

        doc = make_dvp_doc(pos, season, weeks, scoring, totals, source_tag="Sleeper per-player weekly (external builder)")
        # 4) Upload to Worker
        res = upload_to_worker(worker_url, doc)
        log(f"[{pos}] uploaded to Worker: {res}")

    log("\nAll positions processed.")


if __name__ == "__main__":
    sys.exit(main())
