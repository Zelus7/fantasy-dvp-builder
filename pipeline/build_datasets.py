#!/usr/bin/env python3
"""Build and publish league-scoring-aware NFL intelligence datasets."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd
import requests

TEAMS={"ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE","DAL","DEN","DET","GB","HOU","IND","JAX","KC","LAC","LAR","LV","MIA","MIN","NE","NO","NYG","NYJ","PHI","PIT","SEA","SF","TB","TEN","WSH"}
ALIASES={"ARZ":"ARI","BLT":"BAL","CLV":"CLE","HST":"HOU","JAC":"JAX","LA":"LAR","OAK":"LV","SD":"LAC","STL":"LAR","WAS":"WSH"}
POSITIONS=("QB","RB","WR","TE")
WINDOWS={"season":None,"last4":4,"last6":6}
DIRECT={0:("attempts",),1:("completions",),3:("passing_yards",),4:("passing_tds",),19:("passing_2pt_conversions",),20:("interceptions",),23:("carries",),24:("rushing_yards",),25:("rushing_tds",),26:("rushing_2pt_conversions",),41:("receptions",),42:("receiving_yards",),43:("receiving_tds",),44:("receiving_2pt_conversions",),53:("receptions",),58:("targets",),72:("fumbles_lost",)}
CHUNKS={5:(("passing_yards",),5),6:(("passing_yards",),10),7:(("passing_yards",),20),8:(("passing_yards",),25),9:(("passing_yards",),50),10:(("passing_yards",),100),27:(("rushing_yards",),5),28:(("rushing_yards",),10),29:(("rushing_yards",),20),30:(("rushing_yards",),25),31:(("rushing_yards",),50),32:(("rushing_yards",),100),47:(("receiving_yards",),5),48:(("receiving_yards",),10),49:(("receiving_yards",),20),50:(("receiving_yards",),25),51:(("receiving_yards",),50),52:(("receiving_yards",),100),54:(("receptions",),5),55:(("receptions",),10)}
BONUSES={17:(("passing_yards",),300,400),18:(("passing_yards",),400,None),37:(("rushing_yards",),100,200),38:(("rushing_yards",),200,None),56:(("receiving_yards",),100,200),57:(("receiving_yards",),200,None)}


def num(value:Any,default:float=0.0)->float:
    try:
        value=float(value)
        return value if math.isfinite(value) else default
    except (TypeError,ValueError): return default

def clean_id(value:Any)->str|None:
    if value is None or (isinstance(value,float) and math.isnan(value)): return None
    text=str(value).strip()
    if not text or text.lower() in {"nan","none","null"}: return None
    return text[:-2] if text.endswith('.0') and text[:-2].isdigit() else text

def team(value:Any)->str|None:
    raw=str(value or '').strip().upper(); raw=ALIASES.get(raw,raw)
    return raw if raw in TEAMS else None

def position(value:Any)->str|None:
    raw=str(value or '').strip().upper(); raw={"HB":"RB","FB":"RB"}.get(raw,raw)
    return raw if raw in POSITIONS else None

def value(row:Mapping[str,Any],names:Sequence[str])->float:
    for name in names:
        if name in row and row.get(name) is not None:
            result=num(row.get(name),math.nan)
            if math.isfinite(result): return result
    if names==("fumbles_lost",):
        return sum(num(row.get(name)) for name in ("passing_fumbles_lost","rushing_fumbles_lost","receiving_fumbles_lost","sack_fumbles_lost"))
    return 0.0

def score_row(row:Mapping[str,Any],items:Sequence[Mapping[str,Any]])->tuple[float,set[int]]:
    score=0.0; unsupported:set[int]=set()
    for item in items:
        stat=int(num(item.get('statId'),-1)); points=num(item.get('points')); overrides=item.get('overrides') or []
        if not points and not overrides: continue
        if overrides: unsupported.add(stat)
        if stat in DIRECT: score+=value(row,DIRECT[stat])*points
        elif stat in CHUNKS:
            names,divisor=CHUNKS[stat]; score+=math.floor(value(row,names)/divisor)*points
        elif stat in BONUSES:
            names,minimum,maximum=BONUSES[stat]; measured=value(row,names)
            if measured>=minimum and (maximum is None or measured<maximum): score+=points
        elif points: unsupported.add(stat)
    return score,unsupported

def prior_weight(games:int)->float: return {0:1.0,1:.75,2:.60,3:.45,4:.30,5:.15}.get(max(0,int(games)),0.0)
def grade(percentile:float)->str: return 'A' if percentile>=80 else 'B' if percentile>=60 else 'C' if percentile>=40 else 'D' if percentile>=20 else 'F'
def confidence(current:int,prior:int)->float: return round(min(1.0,min(1,current/6)*.82+(.18 if prior>=8 else .1 if prior else 0)),3)
def mean(values:Sequence[float])->float: return statistics.fmean(values) if values else 0.0
def std(values:Sequence[float])->float: return statistics.pstdev(values) if len(values)>1 else 0.0

def to_pandas(frame:Any)->pd.DataFrame:
    if isinstance(frame,pd.DataFrame): return frame.copy()
    if hasattr(frame,'to_pandas'): return frame.to_pandas()
    return pd.DataFrame(frame)

def load_nflreadpy(name:str,seasons:Sequence[int]|None=None)->pd.DataFrame:
    import nflreadpy as nfl
    fn=getattr(nfl,name)
    if seasons is None: return to_pandas(fn())
    for args,kwargs in (((),{'seasons':list(seasons)}),((list(seasons),),{})):
        try: return to_pandas(fn(*args,**kwargs))
        except TypeError: pass
    raise RuntimeError(f'Could not call nflreadpy.{name}')

def player_maps(players:pd.DataFrame)->tuple[dict[str,dict[str,Any]],dict[str,str]]:
    by_gsis={}; espn={}
    for row in players.to_dict('records'):
        gsis=clean_id(row.get('gsis_id') or row.get('player_id'))
        if not gsis: continue
        by_gsis[gsis]=row
        espn_id=clean_id(row.get('espn_id'))
        if espn_id: espn[gsis]=espn_id
    return by_gsis,espn

def scored_rows(frame:pd.DataFrame,players:pd.DataFrame,items:Sequence[Mapping[str,Any]],through:int)->tuple[list[dict[str,Any]],set[int]]:
    by_gsis,_=player_maps(players); output=[]; unsupported:set[int]=set()
    for raw in frame.to_dict('records'):
        week=int(num(raw.get('week'))); gsis=clean_id(raw.get('player_id') or raw.get('gsis_id'))
        if not gsis or week<1 or week>through: continue
        pos=position(raw.get('position') or by_gsis.get(gsis,{}).get('position')); opponent=team(raw.get('opponent_team') or raw.get('opponent'))
        if not pos or not opponent: continue
        points,unknown=score_row(raw,items); unsupported|=unknown
        output.append({**raw,'_id':gsis,'_week':week,'_position':pos,'_opponent':opponent,'_team':team(raw.get('recent_team') or raw.get('team')),'_points':points})
    return output,unsupported

def dvp_rows(current:pd.DataFrame,prior:pd.DataFrame,players:pd.DataFrame,items:Sequence[Mapping[str,Any]],season:int,through:int,all_teams:Iterable[str])->tuple[list[dict[str,Any]],list[int]]:
    now,unknown_now=scored_rows(current,players,items,through); prior_week=int(prior['week'].max()) if not prior.empty and 'week' in prior else 18; old,unknown_old=scored_rows(prior,players,items,max(1,prior_week))
    def games(rows):
        out=defaultdict(float)
        for row in rows: out[(row['_position'],row['_opponent'],row['_week'])]+=num(row['_points'])
        return out
    current_games,prior_games=games(now),games(old); defenses=sorted({team(x) for x in all_teams}-{None}); raw=[]; season_samples={}
    def summarize(source,pos,defense,start,end):
        vals=[points for (p,d,w),points in source.items() if p==pos and d==defense and start<=w<=end]
        return sum(vals),len(vals)
    for pos in POSITIONS:
        for defense in defenses: season_samples[(pos,defense)]=summarize(current_games,pos,defense,1,through)[1]
    for window,length in {'season':None,'last4':4,'last6':6}.items():
        start=1 if length is None else max(1,through-length+1)
        for pos in POSITIONS:
            for defense in defenses:
                current_points,current_count=summarize(current_games,pos,defense,start,through); old_points,old_count=summarize(prior_games,pos,defense,1,prior_week)
                weight=prior_weight(season_samples[(pos,defense)]) if old_count else 0.0
                if not current_count and old_count: weight=1.0
                if not current_count and not old_count: continue
                current_ppg=current_points/current_count if current_count else 0; old_ppg=old_points/old_count if old_count else 0; ppg=current_ppg*(1-weight)+old_ppg*weight; games_used=current_count or old_count
                raw.append({'position':pos,'defenseTeam':defense,'window':window,'season':season,'throughWeek':through,'games':games_used,'currentGames':current_count,'priorGames':old_count,'priorSeason':season-1 if old_count else None,'priorWeight':round(weight,3),'pointsAllowed':round(ppg*games_used,3),'pointsAllowedPerGame':round(ppg,3),'confidence':confidence(current_count,old_count)})
    season_ppg={(r['position'],r['defenseTeam']):r['pointsAllowedPerGame'] for r in raw if r['window']=='season'}; recent_ppg={(r['position'],r['defenseTeam']):r['pointsAllowedPerGame'] for r in raw if r['window']=='last4'}; output=[]
    for window in WINDOWS:
        for pos in POSITIONS:
            rows=sorted([r for r in raw if r['window']==window and r['position']==pos],key=lambda r:(-r['pointsAllowedPerGame'],r['defenseTeam'])); average=mean([r['pointsAllowedPerGame'] for r in rows]); total=len(rows)
            for index,row in enumerate(rows):
                rank=index+1; percentile=100 if total<=1 else 100*(total-rank)/(total-1); baseline=season_ppg.get((pos,row['defenseTeam']),row['pointsAllowedPerGame']); recent=recent_ppg.get((pos,row['defenseTeam']),baseline); delta=(recent-baseline)/baseline if baseline else 0
                output.append({**row,'rank':rank,'percentile':round(percentile,2),'leagueAverageDelta':round((row['pointsAllowedPerGame']-average)/average*100 if average else 0,2),'trend':'worsening' if delta>=.07 else 'improving' if delta<=-.07 else 'stable','grade':grade(percentile)})
    return output,sorted(unknown_now|unknown_old)

def feature_rows(current:pd.DataFrame,prior:pd.DataFrame,players:pd.DataFrame,items:Sequence[Mapping[str,Any]],season:int,through:int)->tuple[list[dict[str,Any]],list[int]]:
    now,u1=scored_rows(current,players,items,through); prior_week=int(prior['week'].max()) if not prior.empty and 'week' in prior else 18; old,u2=scored_rows(prior,players,items,max(1,prior_week)); identities,espn=player_maps(players); by_now=defaultdict(list); by_old=defaultdict(list)
    for row in now: by_now[row['_id']].append(row)
    for row in old: by_old[row['_id']].append(row)
    output=[]
    for gsis in sorted(set(by_now)|set(by_old)):
        if gsis not in espn: continue
        current_rows=sorted(by_now.get(gsis,[]),key=lambda r:r['_week']); old_rows=sorted(by_old.get(gsis,[]),key=lambda r:r['_week']); sample=current_rows or old_rows; pos=position(identities.get(gsis,{}).get('position') or sample[-1]['_position'])
        if not pos: continue
        current_scores=[num(r['_points']) for r in current_rows]; old_scores=[num(r['_points']) for r in old_rows]; weight=prior_weight(len(current_scores)) if old_scores else 0
        if not current_scores and old_scores: weight=1
        current_ppg=mean(current_scores); old_ppg=mean(old_scores); scores=current_scores or old_scores
        targets=[num(r.get('targets')) for r in sample]; carries=[num(r.get('carries')) for r in sample]; receptions=[num(r.get('receptions')) for r in sample]; identity=identities.get(gsis,{})
        output.append({'espnId':espn[gsis],'gsisId':gsis,'playerName':str(identity.get('display_name') or identity.get('full_name') or sample[-1].get('player_display_name') or gsis),'position':pos,'team':team(identity.get('latest_team') or sample[-1].get('_team')),'season':season,'games':len(scores),'currentGames':len(current_scores),'priorGames':len(old_scores),'priorSeason':season-1 if old_scores else None,'priorWeight':round(weight,3),'seasonPpg':round(current_ppg*(1-weight)+old_ppg*weight,3),'last3Ppg':round(mean(scores[-3:]),3),'last5Ppg':round(mean(scores[-5:]),3),'standardDeviation':round(std(scores),3),'targetsPerGame':round(mean(targets),3),'carriesPerGame':round(mean(carries),3),'touchesPerGame':round(mean([carries[i]+receptions[i] for i in range(len(sample))]),3),'targetShare':None})
    return output,sorted(u1|u2)

def schedule_rows(frame:pd.DataFrame,season:int)->list[dict[str,Any]]:
    output=[];seen=set()
    for row in frame.to_dict('records'):
        if int(num(row.get('season'),season))!=season: continue
        week=int(num(row.get('week'))); home=team(row.get('home_team')); away=team(row.get('away_team'))
        if week<1 or week>22 or not home or not away or home==away: continue
        event=clean_id(row.get('game_id') or row.get('event_id')) or f'{season}-{week}-{away}-{home}'
        if event in seen: continue
        seen.add(event); kickoff=None
        try:
            if row.get('gameday') and str(row.get('gameday')).lower()!='nan':
                local=datetime.fromisoformat(f"{row.get('gameday')}T{row.get('gametime') or '00:00'}").replace(tzinfo=ZoneInfo('America/New_York')); kickoff=local.astimezone(timezone.utc).isoformat().replace('+00:00','Z')
        except Exception: pass
        roof=str(row.get('roof') or '').lower()
        output.append({'eventId':event,'season':season,'week':week,'kickoff':kickoff,'homeTeam':home,'awayTeam':away,'venue':None if str(row.get('stadium')).lower()=='nan' else row.get('stadium'),'indoor':roof in {'dome','closed','indoor'},'status':None})
    return sorted(output,key=lambda r:(r['week'],r['kickoff'] or '',r['eventId']))

def now()->str:return datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
def scoring_hash(items)->str:return hashlib.sha256(json.dumps(items,sort_keys=True,separators=(',',':')).encode()).hexdigest()[:16]
def upload(base,token,path,payload):
    response=requests.post(f"{base.rstrip('/')}{path}",headers={'Authorization':f'Bearer {token}'},json=payload,timeout=120)
    if not response.ok: raise RuntimeError(f'{path} upload failed {response.status_code}: {response.text[:400]}')
def write(directory:Path,name:str,payload):directory.mkdir(parents=True,exist_ok=True);(directory/name).write_text(json.dumps(payload,indent=2,allow_nan=False)+'\n')

def main()->int:
    parser=argparse.ArgumentParser();parser.add_argument('--season',type=int);parser.add_argument('--through-week',type=int);parser.add_argument('--no-upload',action='store_true');parser.add_argument('--output-dir',default='pipeline/output');args=parser.parse_args();base=os.environ.get('APP_BASE_URL');token=os.environ.get('DATA_INGEST_TOKEN')
    if not base or not token: raise RuntimeError('APP_BASE_URL and DATA_INGEST_TOKEN are required')
    config=requests.get(f"{base.rstrip('/')}/api/internal/pipeline/config",headers={'Authorization':f'Bearer {token}'},timeout=30);config.raise_for_status();leagues=config.json().get('leagues') or []
    if not leagues: raise RuntimeError('No connected football leagues')
    seasons=sorted({int(args.season or league['seasonYear']) for league in leagues});stats=to_pandas(load_nflreadpy('load_player_stats',sorted(set(seasons)|{s-1 for s in seasons})));players=to_pandas(load_nflreadpy('load_players'));schedules=to_pandas(load_nflreadpy('load_schedules',seasons));output=Path(args.output_dir)
    schedule_payloads={}
    for season in seasons:
        rows=schedule_rows(schedules,season)
        if not rows: raise RuntimeError(f'No schedule rows for {season}')
        payload={'metadata':{'season':season,'throughWeek':max(int(args.through_week or l.get('currentWeek') or 1) for l in leagues if int(args.season or l['seasonYear'])==season),'generatedAt':now(),'source':'nflverse schedules via nflreadpy'},'rows':rows};schedule_payloads[season]=payload;write(output,f'nfl-schedule-{season}.json',payload)
        if not args.no_upload: upload(base,token,'/api/internal/pipeline/schedule',payload)
    for league in leagues:
        season=int(args.season or league['seasonYear']);through=max(1,min(22,int(args.through_week or league.get('currentWeek') or 1)));current=stats[stats['season']==season] if 'season' in stats else pd.DataFrame();prior=stats[stats['season']==season-1] if 'season' in stats else pd.DataFrame();all_teams={r['homeTeam'] for r in schedule_payloads[season]['rows']}|{r['awayTeam'] for r in schedule_payloads[season]['rows']};items=league.get('scoringItems') or []
        dvp,unknown1=dvp_rows(current,prior,players,items,season,through,all_teams);features,unknown2=feature_rows(current,prior,players,items,season,through);unknown=sorted(set(unknown1)|set(unknown2));metadata={'leagueId':str(league['leagueId']),'season':season,'throughWeek':through,'generatedAt':now(),'source':'nflverse weekly player stats via nflreadpy','scoringType':league.get('scoringType'),'scoringHash':scoring_hash(items),'unsupportedScoring':[{'statId':x,'reason':'weekly feed cannot reproduce this rule exactly'} for x in unknown],'earlySeasonBlend':'prior season tapers out after six current games'};dvp_payload={'metadata':metadata,'rows':dvp};feature_payload={'metadata':metadata,'rows':features};write(output,f"dvp-{league['leagueId']}-{season}.json",dvp_payload);write(output,f"player-features-{league['leagueId']}-{season}.json",feature_payload)
        if not args.no_upload: upload(base,token,'/api/internal/pipeline/dvp',dvp_payload);upload(base,token,'/api/internal/pipeline/player-features',feature_payload)
        print(json.dumps({'leagueId':league['leagueId'],'season':season,'throughWeek':through,'dvpRows':len(dvp),'featureRows':len(features),'unsupportedScoring':unknown}))
    return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except Exception as error: print(f'pipeline failed: {error}',file=sys.stderr);raise SystemExit(1)
