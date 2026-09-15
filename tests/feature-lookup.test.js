import test from 'node:test';
import assert from 'node:assert/strict';
import {getPlayerFeatureLookup} from '../src/db.js';

test('league-wide feature lookup stays below the D1 parameter limit',async()=>{
  const sizes=[];
  const env={DB:{prepare(sql){return{bind(...args){assert.ok(args.length<=100,'D1 supports at most 100 bound parameters');return{async first(){return{datasetId:'test-snapshot'}},async all(){assert.match(sql,/FROM player_features/);sizes.push(args.length);return{results:args.slice(1).map(espnId=>({espnId}))}}}}}}}};
  const ids=Array.from({length:150},(_,i)=>String(i));
  const result=await getPlayerFeatureLookup(env,'1269378',2026,[...ids,ids[0]]);
  assert.equal(Object.keys(result).length,150);
  assert.deepEqual(sizes,[91,61]);
});
