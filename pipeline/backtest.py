#!/usr/bin/env python3
"""Chronological benchmark of historical fallbacks, never ESPN projections.

Select a model on 2024 only; report a separate 2025 holdout. Every prediction
uses regular-season rows strictly before its target week plus the prior season.
The scored population is known-history offensive players with a recorded stat
line in the target week. This cannot evaluate DNP/inactive prediction or trades.
"""
from __future__ import annotations
import argparse
import json
from collections import defaultdict
from pathlib import Path
import statistics
import pandas as pd
from pipeline.build_datasets import load_nflreadpy,scored_rows,feature_rows,dvp_rows,TEAMS

MODELS=('season_baseline','recent_blend','adjusted_blend')
def predictions(feature,dvp):
    season=feature['seasonPpg'];recent=feature['last3Ppg'];base=.55*recent+.45*season
    pos=feature['position'];targets=feature['targetsPerGame'];touches=feature['touchesPerGame']
    role=0
    if pos in ('WR','TE'): role=.055 if targets>=9 else .03 if targets>=7 else -.07 if 0<targets<=3.5 else 0
    if pos=='RB': role=.055 if touches>=18 else .025 if touches>=13 else -.08 if 0<touches<=7 else 0
    trend=max(-.08,min(.08,(recent-season)/season*.12)) if season>0 else 0
    matchup=(dvp['percentile']-50)/50*.13*dvp['confidence'] if dvp else 0
    return dict(zip(MODELS,(season,base,base*(1+max(-.28,min(.28,role+trend+matchup))))))

def evaluate(stats,players,items,season):
    current=stats[stats.season==season];prior=stats[stats.season==season-1]
    observations=[]
    for week in range(1,19):
        # Only past weeks enter either feature/DvP constructor.
        features,_=feature_rows(current,prior,players,items,season,week-1)
        feature_map={f['gsisId']:f for f in features if f['games']>=3}
        dvp,_=dvp_rows(current,prior,players,items,season,week-1,TEAMS)
        dvp_map={(r['position'],r['defenseTeam']):r for r in dvp if r['window']=='season'}
        targets,_=scored_rows(current,players,items,week)
        for row in targets:
            if row['_week']!=week or row['_id'] not in feature_map: continue
            feature=feature_map[row['_id']]
            observations.append({'season':season,'week':week,'position':row['_position'],'id':row['_id'],'actual':row['_points'],'predictions':predictions(feature,dvp_map.get((row['_position'],row['_opponent'])))})
    return observations

def metrics(rows,model):
    errors=[r['predictions'][model]-r['actual'] for r in rows]
    return {'n':len(errors),'mae':round(statistics.fmean(abs(e) for e in errors),3) if errors else None,'medianAbsoluteError':round(statistics.median(abs(e) for e in errors),3) if errors else None,'bias':round(statistics.fmean(errors),3) if errors else None}

def report(observations):
    # Same population for all models; top historical baseline players per week.
    buckets=defaultdict(list)
    for row in observations: buckets[(row['week'],row['position'])].append(row)
    relevant=[]
    for (_,pos),rows in buckets.items(): relevant.extend(sorted(rows,key=lambda r:r['predictions']['season_baseline'],reverse=True)[:24 if pos in ('RB','WR') else 12])
    return {'all':{m:metrics(observations,m) for m in MODELS},'lineupRelevant':{m:metrics(relevant,m) for m in MODELS},'byPosition':{p:{m:metrics([r for r in relevant if r['position']==p],m) for m in MODELS} for p in ('QB','RB','WR','TE')}}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config-file',required=True);parser.add_argument('--output-dir',default='.artifacts/backtest');args=parser.parse_args()
    items=json.loads(Path(args.config_file).read_text())['leagues'][0]['scoringItems']
    output=Path(args.output_dir);output.mkdir(parents=True,exist_ok=True)
    cache=output/'stats.parquet';identity=output/'players.parquet'
    if not cache.exists(): load_nflreadpy('load_player_stats',[2023,2024,2025]).to_parquet(cache)
    if not identity.exists(): load_nflreadpy('load_players').to_parquet(identity)
    stats=pd.read_parquet(cache);players=pd.read_parquet(identity)
    development=evaluate(stats,players,items,2024);holdout=evaluate(stats,players,items,2025)
    dev=report(development);test=report(holdout)
    selected=min(MODELS,key=lambda m:dev['lineupRelevant'][m]['mae'])
    result={'protocol':'2024 development, untouched 2025 chronological holdout; prior-year and strictly prior-week data only','selectedOnDevelopment':selected,'development':dev,'holdout':test,'limitations':['Only players with at least three historical stat lines and a target-week stat line; excludes DNP prediction and unseen rookies.','Not a backtest of ESPN projections, injury/news timing, league transactions, trade acceptance or future outcomes.','League rules unsupported by the weekly feed remain approximate.','No calibrated probability or prediction-interval coverage claim.']}
    (output/'report.json').write_text(json.dumps(result,indent=2)+'\n')
    (output/'holdout-predictions.json').write_text(json.dumps(holdout)+'\n')
    print(json.dumps({'selectedOnDevelopment':selected,'development':dev['lineupRelevant'],'holdout':test['lineupRelevant'],'report':str(output/'report.json')},indent=2))
if __name__=='__main__':main()
