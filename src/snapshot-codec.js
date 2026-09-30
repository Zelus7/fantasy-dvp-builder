import {sha256} from './security.js';

// Versioned storage only: hashes and replay still use the exact original JSON.
export const SNAPSHOT_PREFIX='fcc:gzip:v1:';
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

export async function decodeSnapshot(stored,expectedHash){
  if(typeof stored!=='string'||stored.length>MAX_SNAPSHOT_BYTES)throw new Error('Invalid decision snapshot size.');
  let json=stored;
  if(stored.startsWith(SNAPSHOT_PREFIX)){
    const binary=atob(stored.slice(SNAPSHOT_PREFIX.length));
    const bytes=Uint8Array.from(binary,c=>c.charCodeAt(0));
    const plain=await boundedBytes(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')),MAX_SNAPSHOT_BYTES);
    json=new TextDecoder('utf-8',{fatal:true}).decode(plain);
  }else if(encoder.encode(json).length>MAX_SNAPSHOT_BYTES)throw new Error('Invalid decision snapshot size.');
  if(expectedHash&&await sha256(json)!==expectedHash)throw new Error('Decision snapshot integrity check failed.');
  return json;
}

export async function readSnapshot(stored,expectedHash){return JSON.parse(await decodeSnapshot(stored,expectedHash));}
