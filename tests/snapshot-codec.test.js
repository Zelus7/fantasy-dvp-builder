import test from 'node:test';
import assert from 'node:assert/strict';
import {gzipSync} from 'node:zlib';
import {encodeSnapshot,decodeSnapshot,readSnapshot,SNAPSHOT_PREFIX,MAX_SNAPSHOT_BYTES} from '../src/snapshot-codec.js';
import {sha256} from '../src/security.js';

test('snapshot codec preserves exact legacy and compressed JSON bytes and hashes',async()=>{
  for(const raw of ['{"roster":[], "freeAgents":[]}',JSON.stringify({roster:Array.from({length:500},(_,i)=>({id:i,name:'Player ü 🏈',projection:11.2,news:'Test '.repeat(100)}))})]){
    const hash=await sha256(raw),stored=await encodeSnapshot(raw);
    assert.equal(await decodeSnapshot(stored,hash),raw);
    assert.equal(await decodeSnapshot(raw,hash),raw);
    assert.deepEqual(await readSnapshot(stored,hash),JSON.parse(raw));
    if(raw.length>1024){assert.ok(stored.startsWith(SNAPSHOT_PREFIX));assert.ok(stored.length<raw.length/5);}
  }
});
test('snapshot codec rejects corruption, unsupported formats and oversized expansion',async()=>{
  await assert.rejects(decodeSnapshot('{}','wrong-hash'),/integrity/);
  await assert.rejects(readSnapshot('fcc:gzip:v2:unknown'));
  await assert.rejects(decodeSnapshot(SNAPSHOT_PREFIX+'invalid!'));
  const bomb=SNAPSHOT_PREFIX+gzipSync(Buffer.alloc(MAX_SNAPSHOT_BYTES+1,65)).toString('base64');
  await assert.rejects(decodeSnapshot(bomb),/limit/);
  await assert.rejects(encodeSnapshot('x'.repeat(MAX_SNAPSHOT_BYTES+1)),/size/);
  await assert.rejects(decodeSnapshot('ü'.repeat(MAX_SNAPSHOT_BYTES/2+1)),/size/);
});
