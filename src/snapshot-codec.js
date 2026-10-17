import {sha256} from './security.js';

// Versioned storage only: hashes and replay still use the exact original JSON.
export const SNAPSHOT_PREFIX='fcc:gzip:v1:';
export const DELTA_PREFIX='fcc:delta:v1:';
export const MAX_SNAPSHOT_BYTES=8*1024*1024;
const encoder=new TextEncoder();

async function boundedBytes(stream,limit){
  const reader=stream.getReader(),chunks=[];let length=0;
  try{
    while(true){
      const {done,value}=await reader.read();if(done)break;
      length+=value.byteLength;
      if(length>limit){await reader.cancel();throw new Error('Decision snapshot exceeds storage safety limit.');}
      chunks.push(value);
    }
  }finally{reader.releaseLock();}
  const bytes=new Uint8Array(length);let offset=0;
  for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}
  return bytes;
}

export async function encodeSnapshot(json){
  if(typeof json!=='string'||json.length>MAX_SNAPSHOT_BYTES)throw new Error('Invalid decision snapshot size.');
  const bytes=encoder.encode(json);
  if(bytes.length>MAX_SNAPSHOT_BYTES)throw new Error('Invalid decision snapshot size.');
  if(bytes.length<1024)return json;
  const compressed=await boundedBytes(new Blob([bytes]).stream().pipeThrough(new CompressionStream('gzip')),MAX_SNAPSHOT_BYTES+65536);
  let binary='';for(let i=0;i<compressed.length;i+=8192)binary+=String.fromCharCode(...compressed.subarray(i,i+8192));
  const stored=SNAPSHOT_PREFIX+btoa(binary);
  return stored.length<bytes.length?stored:json;
}

export async function decodeSnapshot(stored,expectedHash,{loadBase}={}){
  if(typeof stored!=='string'||stored.length>MAX_SNAPSHOT_BYTES)throw new Error('Invalid decision snapshot size.');
  let json=stored;
  if(stored.startsWith(DELTA_PREFIX)){
    if(typeof loadBase!=='function')throw new Error('Decision snapshot base reader is required.');
    const payload=stored.slice(DELTA_PREFIX.length);
    if(payload.startsWith(DELTA_PREFIX))throw new Error('Nested decision snapshot deltas are not supported.');
    const delta=JSON.parse(await decodeSnapshot(payload));
    if(!delta||typeof delta.baseId!=='string'||!delta.baseId.length||delta.baseId.length>100||!/^[A-Za-z0-9_-]{43}$/.test(delta.baseHash)||!Array.isArray(delta.ops)||delta.ops.length>20000)throw new Error('Invalid decision snapshot delta.');
    const base=await loadBase(delta.baseId);
    if(!base||base.inputHash!==delta.baseHash||typeof base.inputsJson!=='string'||base.inputsJson.startsWith(DELTA_PREFIX))throw new Error('Decision snapshot base is missing or invalid.');
    const value=JSON.parse(await decodeSnapshot(base.inputsJson,delta.baseHash));
    json=JSON.stringify(applyDelta(value,delta.ops));
    if(encoder.encode(json).length>MAX_SNAPSHOT_BYTES)throw new Error('Decision snapshot exceeds storage safety limit.');
  }else if(stored.startsWith(SNAPSHOT_PREFIX)){
    const binary=atob(stored.slice(SNAPSHOT_PREFIX.length));
    const bytes=Uint8Array.from(binary,c=>c.charCodeAt(0));
    const plain=await boundedBytes(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')),MAX_SNAPSHOT_BYTES);
    json=new TextDecoder('utf-8',{fatal:true}).decode(plain);
  }else if(encoder.encode(json).length>MAX_SNAPSHOT_BYTES)throw new Error('Invalid decision snapshot size.');
  if(expectedHash&&await sha256(json)!==expectedHash)throw new Error('Decision snapshot integrity check failed.');
  return json;
}

export async function readSnapshot(stored,expectedHash,options){return JSON.parse(await decodeSnapshot(stored,expectedHash,options));}

// A delta references ONE immutable, independently readable full snapshot. There
// are no dependency chains. Paths replace existing values only; changed key order
// or array length replaces the subtree, preserving JSON.stringify byte order.
function changes(before,after,path=[],depth=0){
  if(depth>64)throw new Error('Decision snapshot nesting exceeds safety limit.');
  if(before===after)return [];
  const replacement=[[path,after]];
  if(!before||!after||typeof before!=='object'||typeof after!=='object'||Array.isArray(before)!==Array.isArray(after))return replacement;
  const keys=Object.keys(before),nextKeys=Object.keys(after);
  if(keys.length!==nextKeys.length||keys.some((key,i)=>key!==nextKeys[i]))return replacement;
  const ops=[];
  for(const key of keys){ops.push(...changes(before[key],after[key],[...path,key],depth+1));if(ops.length>20000)return replacement;}
  return JSON.stringify(ops).length<JSON.stringify(replacement).length?ops:replacement;
}

function applyDelta(value,ops){
  for(const op of ops){
    if(!Array.isArray(op)||op.length!==2||!Array.isArray(op[0])||op[0].length>64)throw new Error('Invalid decision snapshot delta operation.');
    const [path,next]=op;
    if(!path.length){value=next;continue;}
    let parent=value;
    for(let i=0;i<path.length-1;i++){
      const key=path[i];
      if(typeof key!=='string'||!parent||typeof parent!=='object'||!Object.hasOwn(parent,key))throw new Error('Invalid decision snapshot delta path.');
      parent=parent[key];
    }
    const key=path.at(-1);
    if(typeof key!=='string'||!parent||typeof parent!=='object'||!Object.hasOwn(parent,key))throw new Error('Invalid decision snapshot delta path.');
    Object.defineProperty(parent,key,{value:next,writable:true,enumerable:true,configurable:true});
  }
  return value;
}

export async function encodeDeltaSnapshot(json,base){
  const full=await encodeSnapshot(json);
  if(!base||json.length<8192||base.inputsJson.startsWith(DELTA_PREFIX))return full;
  const original=await decodeSnapshot(base.inputsJson,base.inputHash),before=JSON.parse(original),after=JSON.parse(json);
  if(JSON.stringify(before)!==original||JSON.stringify(after)!==json)return full;
  const ops=changes(before,after);
  const payload=JSON.stringify({baseId:base.id,baseHash:base.inputHash,ops});
  if(encoder.encode(payload).length>MAX_SNAPSHOT_BYTES)return full;
  const stored=DELTA_PREFIX+await encodeSnapshot(payload);
  if(encoder.encode(stored).length>=encoder.encode(full).length*.9)return full;
  if(await decodeSnapshot(stored,await sha256(json),{loadBase:async id=>id===base.id?base:null})!==json)throw new Error('Decision snapshot delta roundtrip failed.');
  return stored;
}
