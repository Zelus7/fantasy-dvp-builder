// Offline preparation/verification only. Never connects to production or reads credentials.
// Restore an approved D1 export to SQLite, deploy the compatible reader, then apply
// these bounded SQL files with Wrangler. Keep the generated files private.
import {DatabaseSync} from 'node:sqlite';
import {mkdir,writeFile} from 'node:fs/promises';
import {resolve} from 'node:path';
import {encodeSnapshot,decodeSnapshot,SNAPSHOT_PREFIX} from '../src/snapshot-codec.js';

const [command,source,target]=process.argv.slice(2);
if(!['prepare','verify'].includes(command)||!source||!target)throw new Error('Usage: node scripts/compact-decision-snapshots.mjs prepare backup.sqlite output-dir | verify before.sqlite after.sqlite');
const db=new DatabaseSync(source,{readOnly:true});
const check=database=>{if(database.prepare('PRAGMA integrity_check').get().integrity_check!=='ok')throw new Error('SQLite integrity check failed');};
check(db);
const quote=value=>`'${String(value).replaceAll("'","''")}'`;
const rows=db.prepare('SELECT * FROM decision_snapshots ORDER BY length(inputs_json),id').all();
try{
  if(command==='verify'){
    const after=new DatabaseSync(target,{readOnly:true});
    try{
      check(after);
      let verified=0;
      for(const before of rows){
        const next=after.prepare('SELECT * FROM decision_snapshots WHERE id=?').get(before.id);
        if(!next)throw new Error('Historical snapshot missing');
        const original=await decodeSnapshot(before.inputs_json,before.input_hash);
        if(await decodeSnapshot(next.inputs_json,next.input_hash)!==original)throw new Error('Snapshot roundtrip mismatch');
        for(const key of Object.keys(before))if(key!=='inputs_json'&&before[key]!==next[key])throw new Error(`Historical snapshot metadata changed: ${key}`);
        verified++;
      }
      // These are immutable; operational cache, visits, and datasets can legitimately advance.
      const tables=db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('claim_plan_versions','claim_receipts','espn_claim_observations') ORDER BY name").all();
      let historyRows=0;
      for(const {name} of tables){
        const previous=db.prepare(`SELECT * FROM "${name}" ORDER BY rowid`).all();
        const current=after.prepare(`SELECT * FROM "${name}" ORDER BY rowid`).all();
        const present=new Set(current.map(row=>JSON.stringify(row)));
        for(const row of previous)if(!present.has(JSON.stringify(row)))throw new Error(`Historical record missing or changed: ${name}`);
        historyRows+=previous.length;
      }
      console.log(JSON.stringify({verifiedSnapshots:verified,preservedHistoryRows:historyRows,historyTables:tables.map(t=>t.name),integrity:'ok'}));
    }finally{after.close();}
  }else{
    const directory=resolve(target);await mkdir(directory,{recursive:true,mode:0o700});
    const entries=[];let beforeBytes=0,afterBytes=0;
    for(const row of rows){
      const raw=await decodeSnapshot(row.inputs_json,row.input_hash),stored=await encodeSnapshot(raw);
      if(await decodeSnapshot(stored,row.input_hash)!==raw)throw new Error('Compression roundtrip mismatch');
      beforeBytes+=Buffer.byteLength(row.inputs_json);afterBytes+=Buffer.byteLength(stored);
      if(stored===row.inputs_json||!stored.startsWith(SNAPSHOT_PREFIX))continue;
      const condition=`id=${quote(row.id)} AND input_hash=${quote(row.input_hash)} AND length(CAST(inputs_json AS BLOB))=${Buffer.byteLength(row.inputs_json)} AND substr(inputs_json,1,${SNAPSHOT_PREFIX.length})!=${quote(SNAPSHOT_PREFIX)}`;
      const direct=`UPDATE decision_snapshots SET inputs_json=${quote(stored)} WHERE ${condition};`;
      let sql;
      if(Buffer.byteLength(direct)<90000)sql=direct;
      else{
        // D1 SQL statements are limited to 100 KB. Assemble larger values in a
        // staging table; never expose a half-written historical snapshot.
        const chunks=[];for(let i=0;i<stored.length;i+=60000)chunks.push(stored.slice(i,i+60000));
        sql=[
          'CREATE TABLE IF NOT EXISTS fcc_snapshot_compaction_v1 (id TEXT PRIMARY KEY,payload TEXT NOT NULL);',
          `INSERT OR REPLACE INTO fcc_snapshot_compaction_v1 (id,payload) VALUES (${quote(row.id)},'');`,
          ...chunks.map(chunk=>`UPDATE fcc_snapshot_compaction_v1 SET payload=payload||${quote(chunk)} WHERE id=${quote(row.id)};`),
          `UPDATE decision_snapshots SET inputs_json=(SELECT payload FROM fcc_snapshot_compaction_v1 WHERE id=${quote(row.id)}) WHERE ${condition} AND (SELECT length(payload) FROM fcc_snapshot_compaction_v1 WHERE id=${quote(row.id)})=${stored.length};`,
          `DELETE FROM fcc_snapshot_compaction_v1 WHERE id=${quote(row.id)};`
        ].join('\n');
      }
      const filename=`${String(entries.length).padStart(4,'0')}.sql`;
      await writeFile(resolve(directory,filename),sql+'\n',{mode:0o600,flag:'wx'});
      entries.push({file:filename,id:row.id,beforeBytes:Buffer.byteLength(row.inputs_json),afterBytes:Buffer.byteLength(stored)});
    }
    await writeFile(resolve(directory,'manifest.json'),JSON.stringify({version:1,source:resolve(source),snapshotCount:rows.length,beforeBytes,afterBytes,entries},null,2),{mode:0o600,flag:'wx'});
    console.log(JSON.stringify({snapshots:rows.length,updates:entries.length,beforeBytes,afterBytes,directory}));
  }
}finally{db.close();}
