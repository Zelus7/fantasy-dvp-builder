import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {DatabaseSync} from 'node:sqlite';
import {freezeDecision,decisionHistory} from '../src/decision-store.js';
import {sha256} from '../src/security.js';
import {evaluateFrozenDecision} from '../src/decision-outcomes.js';
import {WAIVER_METHOD} from '../src/waiver-plan.js';
import {normalizeWinningBids} from '../src/espn.js';
import {createHash} from 'node:crypto';
import {MODEL_REVISION} from '../src/model-revision.js';

test('archived model revision fingerprints the exact calculation sources',()=>{
  const hash=createHash('sha256');for(const name of ['analysis','waiver-plan','waiver-market','decision-outcomes','constants','weights','forecast'])hash.update(readFileSync(new URL(`../src/${name}.js`,import.meta.url)));
  hash.update(readFileSync(new URL('../pipeline/models/opportunity-v1.json',import.meta.url)));
  assert.equal(MODEL_REVISION,hash.digest('hex'));
});

test('frozen inputs are server-owned, hash-verifiable, deduplicated and immutable',async t=>{
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());
  for(const f of ['0001_initial.sql','0002_decision_tracking.sql'])db.exec(readFileSync(new URL(`../migrations/${f}`,import.meta.url),'utf8'));
  const env={DB:{prepare(sql){const make=(a=[])=>({bind(...v){return make(v)},async first(){return db.prepare(sql).get(...a)||null},async all(){return {results:db.prepare(sql).all(...a)}},async run(){return db.prepare(sql).run(...a)}});return make()}}};
  const league={leagueId:'1',seasonYear:2026},input={waiversReady:true,roster:[{playerId:'1',projectedPoints:10}],freeAgents:[],settings:{currentWeek:2,mode:'week',now:Date.now()}};
  const first=await freezeDecision(env,league,input,{}),second=await freezeDecision(env,league,input,{});
  assert.equal(first.id,second.id);assert.equal((await decisionHistory(env,league)).length,1);
  const row=db.prepare('SELECT * FROM decision_snapshots').get();assert.equal(row.input_hash,await sha256(row.inputs_json));
  input.roster[0].projectedPoints=12;const changed=await freezeDecision(env,league,input,{});assert.notEqual(changed.id,first.id);
  assert.equal(JSON.parse(db.prepare('SELECT inputs_json FROM decision_snapshots WHERE id=?').get(first.id).inputs_json).roster[0].projectedPoints,10);
});
test('unknown historical scores are not fabricated zeroes',()=>{
  const p={playerId:'1',name:'Receiver',position:'WR',median:10,projectedPoints:10,seasonProjectedPoints:170,eligibleSlotIds:[4],isAvailable:true,game:{kickoff:'2099-01-01T00:00:00Z'}};
  const snapshot={method:WAIVER_METHOD,inputs:{roster:[p],freeAgents:[],settings:{mode:'week',currentWeek:2,endWeek:2,slots:{4:1},now:0}}};
  assert.equal(evaluateFrozenDecision(snapshot,{}).status,'awaiting-complete-scores');
  assert.equal(evaluateFrozenDecision(snapshot,{'1':0}).holdPoints,0);
});
test('bid history never counts pending, missing-price, or unidentified transactions',()=>{
  const tx={id:1,type:'WAIVER',status:'EXECUTED',bidAmount:7,items:[{type:'ADD',playerId:1}]};
  const rows=normalizeWinningBids({transactions:[tx,{...tx,id:2,status:'PENDING'},{...tx,id:3,bidAmount:null},tx]},[{playerId:'1',position:'WR'}]);
  assert.equal(rows.length,1);assert.equal(rows[0].amount,7);
});
