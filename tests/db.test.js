import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { normalizeWeekList, normalizeWeights } from '../src/db.js';

test('risk weights are normalized to one',()=>{const value=normalizeWeights({floor:50,median:10,ceiling:40});assert.equal(value.floor,.5);assert.equal(value.median,.1);assert.equal(value.ceiling,.4)});
test('invalid zero risk weights return the product default',()=>{assert.deepEqual(normalizeWeights({floor:0,median:0,ceiling:0}),{floor:.5,median:.1,ceiling:.4})});
test('fantasy playoff weeks are unique and bounded',()=>{assert.deepEqual(normalizeWeekList([17,15,16,17,0,25]),[15,16,17])});
test('migration includes encrypted credential, staged dataset, and schedule tables',()=>{const sql=fs.readFileSync(new URL('../migrations/0001_initial.sql',import.meta.url),'utf8');for(const table of ['espn_credentials','data_snapshots','dvp_stats','player_features','nfl_schedule','devices'])assert.match(sql,new RegExp(`CREATE TABLE IF NOT EXISTS ${table}`))});
