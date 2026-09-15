import test from 'node:test';
import assert from 'node:assert/strict';
import {analyzePlayer,matchupAdjustment,optimizeLineup} from '../src/analysis.js';
const context={opponentLookup:{BUF:{opponent:'MIA',game:{eventId:'future',kickoff:'2099-01-01T00:00:00Z'}}},riskWeights:{floor:.5,median:.1,ceiling:.4}};
const player={playerId:'1',name:'Player',position:'WR',proTeam:'BUF',projectedPoints:15,eligibleSlotIds:[4,23]};
for(const injuryStatus of ['OUT','IR','SUSPENDED'])test(`${injuryStatus} cannot retain positive weekly upside`,()=>{
  const value=analyzePlayer({...player,injuryStatus},context);
  assert.deepEqual([value.floor,value.median,value.ceiling,value.decisionScore],[0,0,0,0]);
  assert.equal(value.eligibleForRecommendation,false);
  assert.equal(optimizeLineup([value],{4:1},[],0).starters.length,0);
});
test('unaffiliated NFL players and bye players are not weekly recommendations',()=>{
  for(const proTeam of ['FA','ATL'])assert.equal(analyzePlayer({...player,proTeam},context).eligibleForRecommendation,false);
});
test('missing estimates are explicitly unavailable, not manufactured projections',()=>{
  const value=analyzePlayer({...player,projectedPoints:null},context);
  assert.equal(value.median,null);assert.equal(value.eligibleForRecommendation,false);
});
test('ESPN weekly projection is not adjusted twice for injury or matchup',()=>{
  const value=analyzePlayer({...player,injuryStatus:'QUESTIONABLE'},{...context,dvpLookup:{'WR:MIA':{percentile:100,confidence:1}}});
  assert.equal(value.median,15);assert.equal(value.adjustments.total,0);
});
test('zero percentile is the worst matchup, not replaced with average',()=>assert.equal(matchupAdjustment(0,1),-.13));
test('historical fallback follows the validated season baseline without untested multipliers',()=>{
  const value=analyzePlayer({...player,projectedPoints:null},{...context,featureLookup:{'1':{games:5,currentGames:5,seasonPpg:10,last3Ppg:25,targetsPerGame:12}},dvpLookup:{'WR:MIA':{percentile:100,confidence:1}}});
  assert.equal(value.median,10);assert.equal(value.adjustments.total,0);
});
test('a negative ESPN projection stays negative and missing is never a zero',()=>{
  assert.equal(analyzePlayer({...player,projectedPoints:-2},context).median,-2);
  assert.equal(analyzePlayer({...player,projectedPoints:null},context).median,null);
});
