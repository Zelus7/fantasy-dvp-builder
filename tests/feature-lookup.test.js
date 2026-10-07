import test from 'node:test';
import assert from 'node:assert/strict';
import {getPlayerFeatureLookup} from '../src/db.js';
import {analyzePlayer} from '../src/analysis.js';

test('context-only kicker storage never becomes a zero historical score or forecast',async()=>{
  const opportunity={contextOnly:true,kicking:{games:4,attempts:11}};
  const env={DB:{prepare(){return{bind(){return{async first(){return{datasetId:'fixture',throughWeek:4}},async all(){return{results:[{espnId:'1',position:'K',games:0,seasonPpg:0,last3Ppg:0,opportunityJson:JSON.stringify(opportunity)}]}}}}}}}};
  const lookup=await getPlayerFeatureLookup(env,'1',2026,['1']);
  assert.equal(lookup['1'].seasonPpg,null);assert.equal(lookup['1'].last3Ppg,null);assert.equal(lookup['1'].dataThroughWeek,4);
  const p=analyzePlayer({playerId:'1',position:'K',proTeam:'IND',projectedPoints:null},{featureLookup:lookup,opponentLookup:{IND:{opponent:'PIT',game:{}}}});
  assert.equal(p.hasEstimate,false);assert.equal(p.median,null);assert.deepEqual(p.feature.opportunity,opportunity);
});

test('league-wide feature lookup stays below the D1 parameter limit',async()=>{
  const sizes=[];
  const env={DB:{prepare(sql){return{bind(...args){assert.ok(args.length<=100,'D1 supports at most 100 bound parameters');return{async first(){return{datasetId:'test-snapshot'}},async all(){assert.match(sql,/FROM player_features/);sizes.push(args.length);return{results:args.slice(1).map(espnId=>({espnId}))}}}}}}}};
  const ids=Array.from({length:150},(_,i)=>String(i));
  const result=await getPlayerFeatureLookup(env,'1269378',2026,[...ids,ids[0]]);
  assert.equal(Object.keys(result).length,150);
  assert.deepEqual(sizes,[91,61]);
});

test('feature lookup preserves nested intelligence evidence and its original dates',async()=>{
  const opportunity={weeklyUsage:[{week:3,targets:0,snapShare:null}],routes:{routesRun:78,sourceUpdatedAt:'2026-09-29T16:00:00Z'},practice:{week:4,reportDate:null},injuries:{reportedAt:'2026-09-28T10:00:00Z',returnDate:null}};
  const env={DB:{prepare(){return{bind(){return{async first(){return{datasetId:'fixture'}},async all(){return{results:[{espnId:'1',opportunityJson:JSON.stringify(opportunity)}]}}}}}}}};
  const lookup=await getPlayerFeatureLookup(env,'1269378',2026,['1']);
  assert.deepEqual(lookup['1'].opportunity,opportunity);
});
