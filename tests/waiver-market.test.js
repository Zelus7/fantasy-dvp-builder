import test from 'node:test';
import assert from 'node:assert/strict';
import {rivalDemand,historicalBidContext,waiverMarketContext} from '../src/waiver-market.js';

const now=Date.parse('2026-09-23T04:00:00Z');
const p=(id,value,extra={})=>({playerId:id,name:id,position:'WR',median:value,projectedPoints:value,eligibleSlotIds:[4,23],lineupSlotId:20,isAvailable:true,injuryStatus:'ACTIVE',...extra});
test('rival analysis excludes our team and reports possible starting upgrades, not intent',()=>{
  const market={yourTeamId:'me',slots:{4:2,20:6},teams:[{id:'me',roster:[p('ours',1)]},{id:'a',name:'Short on receivers',acquisition:{remaining:224,waiverRank:2},roster:[p('star',18),p('injured',0,{injuryStatus:'IR'})]},{id:'b',name:'Deep roster',roster:[p('one',20),p('two',18),p('three',15)]}]};
  const result=rivalDemand(p('target',10),market,now);
  assert.equal(result.teams.length,2);assert.equal(result.teams[0].starterGainUpperBound,10);assert.equal(result.teams[0].remaining,224);
  assert.equal(result.teams[1].couldStart,false);assert.equal(result.teams[1].remaining,null);
  assert.equal(waiverMarketContext(p('target',10),market,now).winProbability,null);
});
test('questionable receivers remain playable; reserve players cannot fill active slots',()=>{
  const result=rivalDemand(p('target',9),{slots:{4:2},teams:[{id:'a',roster:[p('one',20,{injuryStatus:'QUESTIONABLE'}),p('reserve',30,{lineupSlotId:21})]}]},now);
  assert.equal(result.teams[0].starterGainUpperBound,9);assert.deepEqual(result.teams[0].questionable,['one']);
});
test('locked bench players cannot produce a false competing starter',()=>{
  const result=rivalDemand(p('target',12,{game:{kickoff:'2026-09-22T00:00:00Z'}}),{slots:{4:1},teams:[{id:'a',roster:[p('one',5)]}]},now);
  assert.equal(result.teams[0].couldStart,false);
});
test('missing opponent information is explicit',()=>{
  assert.equal(rivalDemand(p('x',10),{},now).available,false);
});
test('bid history retains sparse examples, deduplicates, excludes future and stale records',()=>{
  const bids=[{id:'1',position:'WR',amount:40,at:'2026-09-16T07:00:00Z'},{id:'2',position:'WR',amount:21,at:'2026-09-16T07:00:00Z'}];
  const result=historicalBidContext('WR',{winningBids:[...bids,bids[0],{id:'3',position:'WR',amount:90,at:'2026-10-01'},{id:'4',position:'WR',amount:1,at:'2026-01-01'},{id:'5',position:'RB',amount:69,at:'2026-09-01'},{id:'6',position:'WR',amount:2}]},now);
  assert.equal(result.sampleSize,2);assert.equal(result.median,30.5);assert.equal(result.high,40);assert.equal(result.quality,'small-sample');
  assert.equal(historicalBidContext('TE',{},now).median,null);
});
