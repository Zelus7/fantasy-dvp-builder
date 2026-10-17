import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,readFileSync,copyFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import {encodeSnapshot,encodeDeltaSnapshot,decodeSnapshot,DELTA_PREFIX} from '../src/snapshot-codec.js';
import {readStoredSnapshot,freezeDecision} from '../src/decision-store.js';
import {sha256} from '../src/security.js';

const fixture=()=>({roster:Array.from({length:25},(_,i)=>({playerId:String(i),name:`Player ${i}`,feature:randomBytes(800).toString('hex'),score:12.3})),settings:{now:1,deep:{deep:0}},unicode:'😀 café'});
const baseOf=async raw=>({id:'base',inputsJson:await encodeSnapshot(raw),inputHash:await sha256(raw)});
const envOf=db=>({DB:{prepare(sql){const statement=(args=[])=>({bind(...values){return statement(values)},async first(){return db.prepare(sql).get(...args)||null},async run(){return db.prepare(sql).run(...args)},async all(){return {results:db.prepare(sql).all(...args)}}});return statement();}}});

test('deltas reconstruct exact JSON, key order, Unicode, repeated paths and array changes',async()=>{
  const before=fixture(),raw=JSON.stringify(before),base=await baseOf(raw),after=structuredClone(before);
  after.roster[2].score=16;after.settings.now=2;after.settings.deep.deep=null;after.unicode='🏈 café';
  after.roster[3]={score:12.3,name:'Reordered keys',playerId:'3',feature:before.roster[3].feature};
  const next=JSON.stringify(after),stored=await encodeDeltaSnapshot(next,base);
  assert.ok(stored.startsWith(DELTA_PREFIX));assert.ok(stored.length<base.inputsJson.length/3);
  const options={loadBase:async id=>id==='base'?base:null};
  assert.equal(await decodeSnapshot(stored,await sha256(next),options),next);
  after.roster.pop();const shorter=JSON.stringify(after),encoded=await encodeDeltaSnapshot(shorter,base);
  assert.equal(await decodeSnapshot(encoded,await sha256(shorter),options),shorter);
  assert.equal(await decodeSnapshot(await encodeDeltaSnapshot(' {"x":1} ',base)), ' {"x":1} ');
  await assert.rejects(decodeSnapshot(stored,await sha256(next)),/base reader/);
  await assert.rejects(decodeSnapshot(stored,await sha256(next),{loadBase:async()=>null}),/missing/);
  await assert.rejects(decodeSnapshot(stored,'wrong',options),/integrity/);
  await assert.rejects(decodeSnapshot(stored,null,{loadBase:async()=>({...base,inputsJson:stored})}),/invalid/);
  await assert.rejects(decodeSnapshot(stored,null,{loadBase:async()=>({...base,inputsJson:'{}'})}),/integrity/);
});

test('delta decoder rejects invalid paths and never mutates object prototypes',async()=>{
  const base=await baseOf(JSON.stringify({safe:1}));
  for(const path of [['__proto__','polluted'],['missing'],[null],['safe','nested']]){
    const encoded=DELTA_PREFIX+JSON.stringify({baseId:base.id,baseHash:base.inputHash,ops:[[path,true]]});
    await assert.rejects(decodeSnapshot(encoded,null,{loadBase:async()=>base}),/path/);
  }
  assert.equal({}.polluted,undefined);
  const original='{"__proto__":{"safe":1}}',ownBase=await baseOf(original);
  const own=DELTA_PREFIX+JSON.stringify({baseId:'base',baseHash:ownBase.inputHash,ops:[[['__proto__','safe'],2]]});
  assert.equal(await decodeSnapshot(own,null,{loadBase:async()=>ownBase}),' {"__proto__":{"safe":2}}'.trim());
  assert.equal({}.safe,undefined);
});

test('new snapshots use only full same-scope anchors; scoped replay rejects cross-league dependencies',async t=>{
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());
  for(const f of ['0001_initial.sql','0002_decision_tracking.sql'])db.exec(readFileSync(new URL(`../migrations/${f}`,import.meta.url),'utf8'));
  const env=envOf(db),league={leagueId:'1',seasonYear:2026},data=fixture();
  const input={waiversReady:true,roster:data.roster,freeAgents:[],settings:{currentWeek:6,now:Date.now(),mode:'week'}};
  const first=await freezeDecision(env,league,input,{});
  input.roster[0].score=20;const second=await freezeDecision(env,league,input,{});
  input.roster[1].score=21;const third=await freezeDecision(env,league,input,{});
  for(const snapshot of [second,third]){
    const row=db.prepare('SELECT inputs_json AS inputsJson,input_hash AS inputHash FROM decision_snapshots WHERE id=?').get(snapshot.id);
    assert.ok(row.inputsJson.startsWith(DELTA_PREFIX));
    assert.equal((await readStoredSnapshot(env,league,row)).roster[0].score,20);
    await assert.rejects(readStoredSnapshot(env,{leagueId:'2',seasonYear:2026},row),/missing/);
  }
  assert.ok(!db.prepare('SELECT inputs_json FROM decision_snapshots WHERE id=?').get(first.id).inputs_json.startsWith(DELTA_PREFIX));
});

test('offline repair is lossless, bounded and idempotent and protects full anchors',async t=>{
  const dir=mkdtempSync(join(tmpdir(),'fcc-delta-test-'));t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const source=join(dir,'before.sqlite'),target=join(dir,'after.sqlite'),out=join(dir,'sql');
  const db=new DatabaseSync(source);
  for(const f of ['0001_initial.sql','0002_decision_tracking.sql'])db.exec(readFileSync(new URL(`../migrations/${f}`,import.meta.url),'utf8'));
  const data=fixture();
  for(let i=0;i<3;i++){
    data.settings.now=i;const raw=JSON.stringify(data);
    db.prepare('INSERT INTO decision_snapshots (id,league_id,season,week,mode,method,created_at,inputs_json,input_hash,result_json) VALUES (?,?,?,?,?,?,?,?,?,?)').run(`s${i}`,'1',2026,6,'week','method',`2020-10-16T01:00:0${i}Z`,await encodeSnapshot(raw),await sha256(raw),'{}');
  }
  for(let i=3;i<5;i++){data.settings.now=i;const current=JSON.stringify(data);db.prepare('INSERT INTO decision_snapshots (id,league_id,season,week,mode,method,created_at,inputs_json,input_hash) VALUES (?,?,?,?,?,?,?,?,?)').run(`live-anchor-${i}`,'1',2026,6,'week','method',new Date().toISOString(),await encodeSnapshot(current),await sha256(current));}
  db.close();copyFileSync(source,target);
  const run=(...args)=>spawnSync(process.execPath,['scripts/deduplicate-decision-snapshots.mjs',...args],{encoding:'utf8'});
  const prepared=run('prepare',source,out);assert.equal(prepared.status,0,prepared.stderr);
  const manifest=JSON.parse(readFileSync(join(out,'manifest.json')));assert.equal(manifest.entries.length,2);
  const next=new DatabaseSync(target);
  for(const entry of manifest.entries){const sql=readFileSync(join(out,entry.file),'utf8');for(const s of sql.split(';\n'))assert.ok(Buffer.byteLength(s)<100000);next.exec(sql);next.exec(sql);}
  next.close();const verified=run('verify',source,target);assert.equal(verified.status,0,verified.stderr);
  assert.equal(JSON.parse(verified.stdout).verifiedSnapshots,5);
  assert.notEqual(run('prepare',target,join(dir,'again')).status,0,'an existing manifest resumes; never rewrite referenced anchors');
  const corrupt=new DatabaseSync(target);corrupt.prepare('UPDATE decision_snapshots SET inputs_json=? WHERE id=?').run('{}','s0');corrupt.close();
  assert.notEqual(run('verify',source,target).status,0);
});
