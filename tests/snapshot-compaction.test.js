import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {randomBytes} from 'node:crypto';
import {mkdtempSync,readFileSync,copyFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import {sha256} from '../src/security.js';

test('offline compaction is bounded, restartable, lossless and refuses corrupted backups',async t=>{
  const dir=mkdtempSync(join(tmpdir(),'fcc-compaction-test-'));t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const source=join(dir,'before.sqlite'),after=join(dir,'after.sqlite'),out=join(dir,'sql');
  const db=new DatabaseSync(source);
  for(const file of ['0001_initial.sql','0002_decision_tracking.sql'])db.exec(readFileSync(new URL(`../migrations/${file}`,import.meta.url),'utf8'));
  for(const [id,raw] of [['small',JSON.stringify({data:'evidence'.repeat(2000)})],['large',JSON.stringify({data:randomBytes(120000).toString('hex')})]]){
    db.prepare('INSERT INTO decision_snapshots (id,league_id,season,week,mode,method,created_at,inputs_json,input_hash,result_json) VALUES (?,?,?,?,?,?,?,?,?,?)').run(id,'1',2026,4,'week','unchanged','unchanged',raw,await sha256(raw),'{"cost":7}');
  }
  db.close();copyFileSync(source,after);
  const run=(...args)=>spawnSync(process.execPath,['scripts/compact-decision-snapshots.mjs',...args],{encoding:'utf8'});
  const prepared=run('prepare',source,out);assert.equal(prepared.status,0,prepared.stderr);
  const manifest=JSON.parse(readFileSync(join(out,'manifest.json')));assert.equal(manifest.entries.length,2);
  const next=new DatabaseSync(after);let staged=false;
  for(const entry of manifest.entries){
    const sql=readFileSync(join(out,entry.file),'utf8');staged||=sql.includes('CREATE TABLE');
    for(const statement of sql.split(';\n'))assert.ok(Buffer.byteLength(statement)<100000);
    next.exec(sql);next.exec(sql);
  }
  next.close();assert.ok(staged,'large snapshots exercise staging instead of oversized SQL');
  const verified=run('verify',source,after);assert.equal(verified.status,0,verified.stderr);
  assert.equal(JSON.parse(verified.stdout).verifiedSnapshots,2);
  const corrupt=new DatabaseSync(after);corrupt.prepare('UPDATE decision_snapshots SET input_hash=? WHERE id=?').run('wrong','small');corrupt.close();
  assert.notEqual(run('prepare',after,join(dir,'bad')).status,0);
  assert.notEqual(run('verify',source,after).status,0);
});
