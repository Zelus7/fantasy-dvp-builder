import test from 'node:test';
import assert from 'node:assert/strict';
import {evaluateTradePlan,discoverTradePlans,tradeWindow} from '../src/trade-plan.js';
import {calendarTradeCard} from '../public/planning-ui.js';
const now=Date.parse('2026-10-06T22:00:00Z');
const make=(playerId,position,value,extra={})=>({playerId,name:playerId,position,team:'BUF',proTeam:'BUF',median:value,projectedPoints:value,seasonProjectedPoints:value*17,hasEstimate:true,isAvailable:true,eligibleForRecommendation:true,injuryStatus:'ACTIVE',lineupSlotId:20,eligibleSlotIds:[position==='RB'?2:position==='TE'?6:position==='QB'?0:4,23],game:{kickoff:'2099-01-01T00:00:00Z'},...extra});
const mine=[make('our lead','RB',20),make('our spare','RB',15),make('our receiver','WR',5)];
const theirs=[make('their lead','WR',20),make('their spare','WR',15),make('their runner','RB',5)];
const options=players=>({now,currentWeek:5,endWeek:8,mode:'bridge',rosterCapacity:3,slots:{2:1,4:1,20:1},schedules:Object.fromEntries(players.map(p=>[p.playerId,{weeks:[5,6,7,8].map(week=>({week,bye:false,missingSchedule:false}))}]))});
test('trade evaluates named before/after starters for both sides over four actual weeks',()=>{
  const r=evaluateTradePlan(mine,theirs,['our spare'],['their spare'],options([...mine,...theirs]));
  assert.equal(r.viable,true);assert.equal(r.yourImpact.gain,40);assert.equal(r.theirImpact.gain,40);
  assert.deepEqual(r.window,{start:5,end:8,count:4});assert.equal(r.yourImpact.weeks[0].after.starters.find(p=>p.slotId===4).name,'their spare');
  const html=calendarTradeCard(r);assert.match(html,/totals across 4/);assert.match(html,/Your team: weekly starters/);assert.doesNotMatch(html,/typical-week starters/);
});
test('a trade that creates a bye-week hole is not recommended despite positive total gain',()=>{
  const opts=options([...mine,...theirs]);opts.schedules['our lead'].weeks[1].bye=true;
  const r=evaluateTradePlan(mine,theirs,['our spare'],['their spare'],opts);
  assert.equal(r.viable,false);assert.match(r.reason,/starting-slot gaps/);assert.equal(r.yourImpact.weeks[1].newGaps,1);
});
test('2-for-1 names required extra drop and includes its lost lineup value',()=>{
  const opts=options([...mine,...theirs]),give=['our spare','our receiver'],receive=['their lead'];
  assert.deepEqual(evaluateTradePlan(mine,theirs,give,receive,opts).requiredDrops,{their:1});
  const result=evaluateTradePlan(mine,theirs,give,receive,{...opts,theirDropIds:['their runner']});
  assert.equal(result.viable,true);assert.equal(result.yourImpact.gain,60);assert.equal(result.theirImpact.gain,20);
  assert.equal(result.extraMoves.your.openSlots,1);assert.equal(result.extraMoves.their.drops[0].name,'their runner');
});
test('conditional follow-up is explicit, ownership checked, and cannot be claimed by both sides',()=>{
  const free=make('free','WR',7,{status:'WAIVERS'}),opts={...options([...mine,...theirs,free]),freeAgents:[free],theirDropIds:['their runner'],yourAddIds:['free']};
  const r=evaluateTradePlan(mine,theirs,['our spare','our receiver'],['their lead'],opts);
  assert.equal(r.viable,true);assert.equal(r.conditional,true);assert.equal(r.extraMoves.your.openSlots,0);
  assert.equal(evaluateTradePlan(mine,theirs,['our spare','our receiver'],['their lead'],{...opts,freeAgents:[{...free,onTeamId:5}]}).viable,false);
  const duplicate=evaluateTradePlan(mine,theirs,['our spare'],['their spare'],{...opts,yourDropIds:['our receiver'],theirAddIds:['free']});
  assert.match(duplicate.reason,/same free agent/);
});
test('incomplete schedules never yield a numeric full-window gain or recommendation',()=>{
  const opts=options([...mine,...theirs]);delete opts.schedules['their spare'];
  const r=evaluateTradePlan(mine,theirs,['our spare'],['their spare'],opts);
  assert.equal(r.viable,false);assert.equal(r.yourImpact.gain,null);assert.match(r.reason,/incomplete/);
});
test('injury bookends preserve unavailable current players and expose differing future returns',()=>{
  const hurt=make('hurt','RB',0,{seasonProjectedPoints:340,isAvailable:false,eligibleForRecommendation:false,injuryStatus:'IR',injuryEvidence:{earliestWeek:6,earlyWeek:6,planningWeek:7,lateWeek:9,reviewBy:'2026-10-08T22:00:00Z'}});
  const own=[hurt,...mine.slice(1)],opts=options([...own,...theirs]);
  const r=evaluateTradePlan(own,theirs,['our spare'],['their spare'],opts);
  assert.equal(r.scenarioResults.length,3);assert.ok(r.scenarioResults.every(s=>!s.your.weeks[0].before.starters.some(p=>p.name==='hurt')));
  assert.notEqual(r.scenarioResults[0].your.gain,r.scenarioResults[2].your.gain);assert.equal(r.viable,false);
});
test('protection, lock, invalid capacity and malformed auxiliary IDs fail closed',()=>{
  const opts=options([...mine,...theirs]);
  for(const extra of [{protectedIds:['our spare']},{rosterCapacity:null},{yourDropIds:{}},{yourAddIds:['bad']}])assert.equal(evaluateTradePlan(mine,theirs,['our spare'],['their spare'],{...opts,...extra}).viable,false);
  const locked=mine.map(p=>({...p,game:{kickoff:'2026-10-01T20:00:00Z'}}));
  assert.match(evaluateTradePlan(locked,theirs,['our spare'],['their spare'],opts).reason,/already started/);
  assert.equal(evaluateTradePlan(locked,theirs,['our spare'],['their spare'],{...opts,effectiveWeek:6}).viable,true);
  for(const [give,receive] of [[{},[]],[['our spare','our spare'],['their spare']],[['their runner'],['their spare']]])assert.equal(evaluateTradePlan(mine,theirs,give,receive,opts).viable,false);
});
test('bounded discovery includes unequal packages, never unverified free-agent assumptions',()=>{
  const r=discoverTradePlans([{id:9,name:'Us',roster:mine},{id:8,name:'Them',roster:theirs}],9,options([...mine,...theirs]));
  assert.ok(r.some(t=>t.give.length===2&&t.receive.length===1));assert.ok(r.every(t=>t.viable&&!t.conditional));
});
test('window dates are bounded, including last regular fantasy week',()=>{
  assert.deepEqual(tradeWindow({currentWeek:17,endWeek:17,mode:'bridge'}),[17]);
  assert.deepEqual(tradeWindow({currentWeek:5,effectiveWeek:4,endWeek:17}),[]);
});
