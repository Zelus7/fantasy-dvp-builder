// Offline, lossless repair. Full anchors remain immutable; every delta has one
// same-league/season full parent. No production connection or credential access.
import {DatabaseSync} from 'node:sqlite';
import {mkdir,writeFile} from 'node:fs/promises';
import {resolve} from 'node:path';
import {encodeDeltaSnapshot,decodeSnapshot,DELTA_PREFIX} from '../src/snapshot-codec.js';

const [command,source,target]=process.argv.slice(2);
if(!['prepare','verify'].includes(command)||!source||!target)throw new Error('Usage: prepare backup.sqlite output-dir | verify before.sqlite after.sqlite');
const db=new DatabaseSync(source,{readOnly:true});
const check=d=>{if(d.prepare('PRAGMA integrity_check').get().integrity_check!=='ok')throw new Error('SQLite integrity check failed');};
const quote=v=>`'${String(v).replaceAll("'","''")}'`;
const baseRow=row=>row&&({id:row.id,inputHash:row.input_hash,inputsJson:row.inputs_json});
const reader=(d,row)=>({loadBase:async id=>baseRow(d.prepare('SELECT id,input_hash,inputs_json FROM decision_snapshots WHERE id=? AND league_id=? AND season=?').get(id,row.league_id,row.season))});
check(db);
const rows=db.prepare('SELECT * FROM decision_snapshots ORDER BY created_at,id').all();
try{
  if(command==='verify'){
    const after=new DatabaseSync(target,{readOnly:true});
    try{
      check(after);let verified=0,historyRows=0;
      for(const row of rows){
        const next=after.prepare('SELECT * FROM decision_snapshots WHERE id=?').get(row.id);
        if(!next)throw new Error('Historical snapshot missing');
        if(await decodeSnapshot(row.inputs_json,row.input_hash,reader(db,row))!==await decodeSnapshot(next.inputs_json,next.input_hash,reader(after,next)))throw new Error('Snapshot bytes changed');
        for(const key of Object.keys(row))if(key!=='inputs_json'&&row[key]!==next[key])throw new Error(`Historical metadata changed: ${key}`);
        verified++;
      }
      const tables=db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('claim_plan_versions','claim_receipts','espn_claim_observations') ORDER BY name").all();
      for(const {name} of tables){
        const present=new Set(after.prepare(`SELECT * FROM "${name}"`).all().map(row=>JSON.stringify(row)));
        for(const row of db.prepare(`SELECT * FROM "${name}"`).all()){if(!present.has(JSON.stringify(row)))throw new Error(`Historical receipt changed: ${name}`);historyRows++;}
      }
      console.log(JSON.stringify({verifiedSnapshots:verified,preservedHistoryRows:historyRows,integrity:'ok'}));
    }finally{after.close();}
  }else{
    // Do not transform archives already referenced by an existing delta. A
    // repeated prepare is intentionally a no-op: no chains or anchor rewriting.
    if(rows.some(row=>row.inputs_json.startsWith(DELTA_PREFIX)))throw new Error('Archive already contains deltas; use the existing manifest to resume, then verify.');
    const directory=resolve(target);await mkdir(directory,{recursive:true,mode:0o700});
    const anchors=new Map(),entries=[],today=new Date().toISOString().slice(0,10);let beforeBytes=0,afterBytes=0;
    for(const row of rows){
      const raw=await decodeSnapshot(row.inputs_json,row.input_hash),key=`${row.league_id}:${row.season}:${row.method}:${row.created_at.slice(0,10)}`;
      // Live writers reference only today's full anchors. Never transform this
      // day's rows, even if a writer starts using one after this export was taken.
      const base=anchors.get(key),stored=base&&row.created_at.slice(0,10)<today?await encodeDeltaSnapshot(raw,base):row.inputs_json;
      beforeBytes+=Buffer.byteLength(row.inputs_json);afterBytes+=Buffer.byteLength(stored);
      if(!stored.startsWith(DELTA_PREFIX)){anchors.set(key,baseRow(row));continue;}
      if(await decodeSnapshot(stored,row.input_hash,{loadBase:async id=>id===base.id?base:null})!==raw)throw new Error('Delta roundtrip failed');
      const condition=`id=${quote(row.id)} AND input_hash=${quote(row.input_hash)} AND length(CAST(inputs_json AS BLOB))=${Buffer.byteLength(row.inputs_json)} AND substr(inputs_json,1,${DELTA_PREFIX.length})!=${quote(DELTA_PREFIX)} AND EXISTS (SELECT 1 FROM decision_snapshots b WHERE b.id=${quote(base.id)} AND b.input_hash=${quote(base.inputHash)} AND substr(b.inputs_json,1,${DELTA_PREFIX.length})!=${quote(DELTA_PREFIX)})`;
      const direct=`UPDATE decision_snapshots SET inputs_json=${quote(stored)} WHERE ${condition};`;
      let statements=[direct];
      if(Buffer.byteLength(direct)>=90000){
        const chunks=[];for(let i=0;i<stored.length;i+=60000)chunks.push(stored.slice(i,i+60000));
        statements=[
          'CREATE TABLE IF NOT EXISTS fcc_snapshot_delta_staging_v1 (id TEXT PRIMARY KEY,payload TEXT NOT NULL);',
          `INSERT OR REPLACE INTO fcc_snapshot_delta_staging_v1 (id,payload) VALUES (${quote(row.id)},'');`,
          ...chunks.map(chunk=>`UPDATE fcc_snapshot_delta_staging_v1 SET payload=payload||${quote(chunk)} WHERE id=${quote(row.id)};`),
          `UPDATE decision_snapshots SET inputs_json=(SELECT payload FROM fcc_snapshot_delta_staging_v1 WHERE id=${quote(row.id)}) WHERE ${condition} AND (SELECT length(CAST(payload AS BLOB)) FROM fcc_snapshot_delta_staging_v1 WHERE id=${quote(row.id)})=${Buffer.byteLength(stored)};`,
          `DELETE FROM fcc_snapshot_delta_staging_v1 WHERE id=${quote(row.id)};`
        ];
      }
      if(statements.some(s=>Buffer.byteLength(s)>=100000))throw new Error('D1 statement limit exceeded');
      const file=`${String(entries.length).padStart(4,'0')}.sql`;
      await writeFile(resolve(directory,file),statements.join('\n')+'\n',{mode:0o600,flag:'wx'});
      entries.push({file,id:row.id,baseId:base.id,beforeBytes:Buffer.byteLength(row.inputs_json),afterBytes:Buffer.byteLength(stored)});
    }
    const manifest={version:1,snapshotCount:rows.length,beforeBytes,afterBytes,fullAnchors:rows.length-entries.length,entries};
    await writeFile(resolve(directory,'manifest.json'),JSON.stringify(manifest,null,2),{mode:0o600,flag:'wx'});
    console.log(JSON.stringify({...manifest,entries:entries.length,directory}));
  }
}finally{db.close();}
