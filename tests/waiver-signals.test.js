import test from 'node:test';
import assert from 'node:assert/strict';
import {waiverRoleSignals,emergingWaiverTargets} from '../src/waiver-signals.js';
import {planWaivers} from '../src/waiver-plan.js';
import {emergingRoleSection} from '../public/planning-ui.js';
const now=Date.parse('2026-10-06T22:00:00Z');
const make=(playerId,position,points,extra={})=>({playerId,name:playerId,position,team:'PHI',proTeam:'PHI',median:points,projectedPoints:points,seasonProjectedPoints:points*17,isAvailable:true,eligibleForRecommendation:true,hasEstimate:true,injuryStatus:'ACTIVE',eligibleSlotIds:[position==='QB'?0:position==='RB'?2:4],game:{kickoff:'2026-10-11T17:00:00Z'},...extra});
const settings={season:2026,currentWeek:5,endWeek:8,now};
test('an injury replacement appears even when projections do not beat current starters',()=>{
  const shipley=make('Shipley','RB',5,{status:'WAIVERS'}),barkley=make('Barkley','RB',0,{injuryStatus:'OUT',percentOwned:99});
  const results=emergingWaiverTargets([shipley],[],{...settings,rolePlayers:[shipley,barkley]});
  assert.equal(results.length,1);assert.equal(results[0].projectedPoints,5);assert.equal(results[0].roleEvidence.contextOnly,true);
  assert.match(results[0].roleEvidence.signals[0].label,/does not establish/);
  const html=emergingRoleSection(results);assert.match(html,/not a claim recommendation/);assert.doesNotMatch(html,/data-plan-player/);
});
test('questionable teammates are not treated as confirmed absences; unrelated teams excluded',()=>{
  const p=make('candidate','RB',5),others=[make('q','RB',12,{injuryStatus:'QUESTIONABLE',percentOwned:99}),make('other','RB',12,{team:'SEA',injuryStatus:'IR',percentOwned:99})];
  assert.equal(waiverRoleSignals(p,others,settings).signals.length,0);
});
test('role rises use strictly earlier same-team games and independently identify targets, carries and snaps',()=>{
  const p=make('riser','RB',8,{feature:{season:2026,opportunity:{throughWeek:4,weeklyUsage:[{week:2,team:'PHI',carries:5,targets:1,snapShare:.2},{week:3,team:'PHI',carries:7,targets:1,snapShare:.3},{week:4,team:'PHI',carries:20,targets:5,snapShare:.65,redZoneOpportunities:4},{week:5,team:'PHI',carries:100,targets:100,snapShare:1}]}}});
  const e=waiverRoleSignals(p,[],settings);assert.equal(e.signals.length,4);assert.ok(e.signals.every(s=>s.week===4));
  assert.equal(waiverRoleSignals(p,[],{...settings,currentWeek:6}).signals.length,0);
  assert.equal(waiverRoleSignals({...p,feature:{...p.feature,season:2025}},[],settings).signals.length,0);
});
test('a single big game without prior observations is not a fabricated trend',()=>{
  const p=make('rookie','WR',8,{feature:{season:2026,opportunity:{throughWeek:4,weeklyUsage:[{week:4,team:'PHI',targets:12}],touchdownDependent:true}}});
  const e=waiverRoleSignals(p,[],settings);assert.equal(e.signals.length,0);assert.match(e.warnings[0],/touchdowns/);
});
test('owned, locked, unavailable and teamless players are excluded from actionable research',()=>{
  const target=make('target','RB',5,{status:'WAIVERS'}),injured=make('star','RB',0,{injuryStatus:'IR',percentOwned:99}),opts={...settings,rolePlayers:[injured]};
  for(const extra of [{onTeamId:9},{injuryStatus:'IR'},{team:'FA'},{game:{kickoff:'2020-01-01'}},{status:'ONTEAM'}])assert.equal(emergingWaiverTargets([{...target,...extra}],[],opts).length,0);
  assert.equal(emergingWaiverTargets([target],[target],opts).length,0);
});
test('equal-gain waiver drop favors surplus backup QB instead of useful receiver depth',()=>{
  const qb=make('Maye','QB',20,{isStarter:true,lineupSlotId:0}),backup=make('Nix','QB',18),wr=make('Vele','WR',10,{isStarter:true,lineupSlotId:4}),bench=make('Meyers','WR',9),add=make('Kyler','QB',21.8,{status:'WAIVERS'});
  const players=[qb,backup,wr,bench,add],opts={...settings,rosterCapacity:4,slots:{0:1,4:1,20:2},schedules:Object.fromEntries(players.map(p=>[p.playerId,{weeks:[5,6,7,8].map(week=>({week,bye:false,missingSchedule:false}))}]))};
  const r=planWaivers([add],players.slice(0,4),opts)[0];
  assert.equal(r.drop.position,'QB');assert.equal(r.actionPlan.startsNow,true);assert.equal(r.actionPlan.displaced[0].name,'Maye');
  assert.match(r.actionPlan.nextMove,/Recheck|Before Week/);
});
test('future-only bye coverage survives a crowded zero-current-gain shortlist',()=>{
  const lead=make('lead','WR',20,{isStarter:true,lineupSlotId:4}),spare=make('spare','WR',2),target=make('z-target','WR',0,{status:'WAIVERS',seasonProjectedPoints:170});
  const filler=Array.from({length:60},(_,i)=>make(`a-${i}`,'WR',0,{status:'WAIVERS',seasonProjectedPoints:17}));
  const players=[lead,spare,target,...filler];
  for(const [mode,bye] of [['bridge',6],['ros',10]]){
    const opts={...settings,mode,endWeek:12,rosterCapacity:2,slots:{4:1,20:1},schedules:Object.fromEntries(players.map(p=>[p.playerId,{weeks:Array.from({length:8},(_,i)=>({week:5+i,bye:p===lead&&5+i===bye,missingSchedule:false}))}]))};
    const result=planWaivers([target,...filler],[lead,spare],opts).find(p=>p.playerId==='z-target');
    assert.ok(result);assert.equal(result.drop.playerId,'spare');assert.equal(result.actionPlan.startsNow,false);assert.ok(result.actionPlan.startWeeks.includes(bye));
  }
});
