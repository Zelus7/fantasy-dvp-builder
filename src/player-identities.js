import {cacheGet} from './db.js';
import {PRO_TEAM_MAP} from './constants.js';

const key=league=>`espn:identities:v1:${league.leagueId}:${league.seasonYear}`;
const playerId=value=>/^-?[1-9]\d{0,11}$/.test(String(value))?String(value):null;
const cleanName=value=>typeof value==='string'&&value.trim().length<=100&&/\p{L}/u.test(value)&&!/[<>\u0000-\u001f]/.test(value)&&!/^Player\s+-?\d+$/i.test(value.trim())?value.trim():null;
const positions=new Set(['QB','RB','WR','TE','K','D/ST']);

// Identity is not roster membership, availability, or evidence of a fresh ESPN sync.
export function mergeIdentities(...groups){
  const byId=new Map();
  for(const group of groups)for(const p of group||[]){
    const id=playerId(p?.playerId),name=cleanName(p?.name);if(!id||!name)continue;
    byId.set(id,{...byId.get(id),playerId:id,name,...(positions.has(p.position)?{position:p.position}:{})});
  }
  return [...byId.values()];
}

async function publicJson(url){
  const response=await fetch(url,{method:'GET',redirect:'manual',headers:{Accept:'application/json'},signal:AbortSignal.timeout(3000)});
  if(!response.ok||response.redirected||Number(response.headers.get('content-length'))>128*1024||!response.body)throw new Error('Identity lookup unavailable');
  const reader=response.body.getReader(),chunks=[];let size=0;
  try{while(true){const {done,value}=await reader.read();if(done)break;size+=value.byteLength;if(size>128*1024)throw new Error('Identity response too large');chunks.push(value);}}
  finally{await reader.cancel().catch(()=>{});}
  const bytes=new Uint8Array(size);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}
  return JSON.parse(new TextDecoder().decode(bytes));
}

export async function fetchPublicIdentity(value){
  const id=playerId(value);if(!id)return null;
  try{
    if(Number(id)<0){
      const teamId=-16000-Number(id);if(teamId<=0||!PRO_TEAM_MAP[teamId])return null;
      const {team}=await publicJson(`https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/${teamId}`);
      if(String(team?.id)!==String(teamId)||!cleanName(team?.name))return null;
      return mergeIdentities([{playerId:id,name:`${team.name} D/ST`,position:'D/ST'}])[0]||null;
    }
    const athlete=await publicJson(`https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/${id}`);
    if(String(athlete?.id)!==id)return null;
    return mergeIdentities([{playerId:id,name:athlete.displayName||athlete.fullName,position:athlete.position?.abbreviation}])[0]||null;
  }catch{return null;}
}

export async function resolvePlayerIdentities(env,league,seeds,ids){
  let retained=[];
  try{const value=(await cacheGet(env,key(league),true))?.value;retained=Object.values(value||{}).slice(0,5000);}catch{/* Names must not block transaction evidence. */}
  let known=mergeIdentities(retained,seeds);
  const byId=new Map(known.map(p=>[p.playerId,p]));
  const wanted=[...new Set(ids.map(playerId).filter(Boolean))],unknown=wanted.filter(id=>!byId.has(id)).slice(0,12);
  // A dropped player may already be known to the normal fantasy waiver feed.
  // Reuse that identity even if availability has expired; never reuse its status.
  if(unknown.length)try{
    const {results}=await env.DB.prepare(`SELECT json_extract(p.value,'$.playerId') AS playerId,
      json_extract(p.value,'$.name') AS name,json_extract(p.value,'$.position') AS position
      FROM api_cache c,json_each(CASE WHEN json_valid(c.payload_json) THEN c.payload_json ELSE '[]' END) p
      WHERE c.cache_key LIKE ? AND CAST(json_extract(p.value,'$.playerId') AS TEXT) IN (${unknown.map(()=>'?').join(',')})
      ORDER BY c.updated_at DESC LIMIT 500`).bind(`espn:v2:freeagents:${league.leagueId}:${league.seasonYear}:%`,...unknown).all();
    known=mergeIdentities((results||[]).toReversed(),known);
  }catch{/* A missing normalized cache is not a failed transaction read. */}
  const cachedIds=new Set(known.map(p=>p.playerId)),missing=unknown.filter(id=>!cachedIds.has(id));
  const found=[];
  for(let i=0;i<missing.length;i+=6)found.push(...await Promise.all(missing.slice(i,i+6).map(fetchPublicIdentity)));
  const result=mergeIdentities(known,found.filter(Boolean)),before=new Map(mergeIdentities(retained).map(p=>[p.playerId,p]));
  const changes=result.filter(p=>JSON.stringify(p)!==JSON.stringify(before.get(p.playerId)));
  let persistence='retained';
  if(changes.length)try{
    // Atomic per-key merge preserves names learned by concurrent requests; receipts stay immutable.
    await env.DB.prepare(`INSERT INTO api_cache (cache_key,payload_json,expires_at,updated_at) VALUES (?,?,?,CURRENT_TIMESTAMP)
      ON CONFLICT(cache_key) DO UPDATE SET payload_json=json_patch(api_cache.payload_json,excluded.payload_json),expires_at=excluded.expires_at,updated_at=CURRENT_TIMESTAMP`)
      .bind(key(league),JSON.stringify(Object.fromEntries(changes.map(p=>[p.playerId,p]))),new Date(Date.now()+365*86400000).toISOString()).run();
  }catch{persistence='unavailable';}
  const resolved=new Set(result.map(p=>p.playerId));
  return {players:result,coverage:{requested:wanted.length,unresolved:wanted.filter(id=>!resolved.has(id)).length,persistence}};
}
