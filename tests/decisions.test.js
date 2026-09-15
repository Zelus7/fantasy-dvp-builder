import test from 'node:test';
import assert from 'node:assert/strict';
import {recommendWaiverMoves,evaluateBilateralTrade,discoverTradeTargets,horizonValue,byeCoverage} from '../src/decisions.js';
const make=(playerId,position,value,extra={})=>({playerId,name:playerId,position,proTeam:'BUF',team:'BUF',median:value,projectedPoints:value,seasonProjectedPoints:value*17,hasEstimate:true,isAvailable:true,eligibleForRecommendation:true,injuryStatus:'ACTIVE',eligibleSlotIds:[position==='RB'?2:4,23],...extra});
const slots={2:1,4:1,20:2};
const mine=[make('my-rb1','RB',20),make('my-rb2','RB',15),make('my-wr','WR',5)];
const theirs=[make('their-wr1','WR',20),make('their-wr2','WR',15),make('their-rb','RB',5)];
test('trade requires real benefit for both complementary rosters',()=>{
  const result=evaluateBilateralTrade(mine,theirs,['my-rb2'],['their-wr2'],{slots});
  assert.equal(result.viable,true);assert.equal(result.yourImpact.gain,10);assert.equal(result.theirImpact.gain,10);
});
test('worst-for-best, duplicate, wrong-owner and protected-player trades are rejected',()=>{
  for(const [give,receive,options] of [[['my-wr'],['their-wr1'],{}],[['my-rb2','my-rb2'],['their-wr2'],{}],[['their-rb'],['my-rb1'],{}],[['my-rb2'],['their-wr2'],{protectedIds:['my-rb2']}]])assert.equal(evaluateBilateralTrade(mine,theirs,give,receive,{slots,...options}).viable,false);
});
test('balanced value is insufficient when one side gets no lineup improvement',()=>{
  assert.equal(evaluateBilateralTrade(mine,[...theirs,make('their-rb2','RB',15)],['my-rb2'],['their-rb2'],{slots}).viable,false);
});
test('discovery returns only bilateral gains and records the counterpart',()=>{
  const results=discoverTradeTargets([{id:'1',name:'Us',roster:mine},{id:'2',name:'Them',roster:theirs}],'1',{slots});
  assert.ok(results.length);assert.ok(results.every(r=>r.viable&&r.yourImpact.gain>=.5&&r.theirImpact.gain>=.5&&r.otherTeam.id==='2'));
});
test('weekly waivers name a legal drop without sacrificing a stronger season starter',()=>{
  const add=make('free-wr','WR',12,{status:'FREEAGENT'});
  const moves=recommendWaiverMoves([add],mine,{slots,now:0});
  assert.equal(moves.length,1);assert.equal(moves[0].drop.playerId,'my-wr');assert.equal(moves[0].lineupGain,7);
  const star=make('resting-star','WR',1,{seasonProjectedPoints:25*17});
  assert.equal(recommendWaiverMoves([add],[star],{slots:{4:1},now:0}).length,0);
});
test('protected, unavailable, already-owned and locked additions cannot be recommended',()=>{
  const add=make('new','WR',12,{status:'FREEAGENT'});
  assert.equal(recommendWaiverMoves([add],mine,{slots,protectedIds:mine.map(p=>p.playerId),now:0}).length,0);
  for(const extra of [{isAvailable:false},{onTeamId:8},{status:'ONTEAM'},{game:{kickoff:'2000-01-01T00:00:00Z'}}])assert.equal(recommendWaiverMoves([{...add,...extra}],mine,{slots}).length,0);
});
test('an open slot does not require an unnecessary drop',()=>{
  const moves=recommendWaiverMoves([make('free','WR',12,{status:'FREEAGENT'})],mine,{slots,rosterCapacity:4,now:0});
  assert.equal(moves[0].drop,null);
});
test('ROS estimate uses season strength rather than a single bad weekly matchup',()=>{
  const p=make('season','WR',2,{seasonProjectedPoints:17*15});
  assert.equal(horizonValue(p,'week'),2);assert.equal(horizonValue(p,'ros'),15);
  assert.equal(horizonValue({...p,injuryStatus:'IR'},'ros'),null);
});
test('IR occupants do not consume a vacant active roster slot',()=>{
  const roster=[...mine,make('ir','WR',20,{injuryStatus:'IR',lineupSlotId:21})];
  const moves=recommendWaiverMoves([make('free','WR',12,{status:'FREEAGENT'})],roster,{slots,rosterCapacity:4,now:0});
  assert.equal(moves[0].drop,null);
});
test('malformed or oversized trade packages are rejected without throwing',()=>{
  assert.equal(evaluateBilateralTrade(mine,theirs,{},[],{slots}).viable,false);
  assert.equal(evaluateBilateralTrade(mine,theirs,['1','2','3'],[],{slots}).viable,false);
});
test('bye planning separates a real coverage gap from missing schedule data',()=>{
  const schedules=Object.fromEntries(mine.map(p=>[p.playerId,{weeks:[{week:10,bye:p.position==='RB',missingSchedule:false},{week:11,bye:false,missingSchedule:true}]}]));
  const plan=byeCoverage(mine,slots,schedules);
  assert.equal(plan[0].status,'coverage-gap');assert.deepEqual(plan[0].unfilledSlots,[2]);assert.equal(plan[1].status,'unknown');
});
