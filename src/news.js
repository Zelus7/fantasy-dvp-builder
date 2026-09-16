import {cacheGet,cachePut} from './db.js';
const KEY='nfl:news';
import {normalizeNews} from './news-format.js';
export {normalizeNews};
export async function fetchNews(env,{force=false}={}){
  const hit=force?null:await cacheGet(env,KEY);if(hit)return hit.value;
  try{
    const response=await fetch('https://site.api.espn.com/apis/site/v2/sports/football/nfl/news?limit=12',{signal:AbortSignal.timeout(5000)});
    if(!response.ok)throw new Error('News unavailable');
    const items=normalizeNews(await response.json());if(!items.length)throw new Error('Empty news feed');
    const value={generatedAt:new Date().toISOString(),items};await cachePut(env,KEY,value,1800);return value;
  }catch{return (await cacheGet(env,KEY,true))?.value||null}
}

export async function fetchPlayerNews(env,players,{force=false}={}){
  const selected=[...players].filter(p=>/^\d{1,12}$/.test(String(p.playerId)))
    .sort((a,b)=>Number(b.injuryStatus!=='ACTIVE')-Number(a.injuryStatus!=='ACTIVE')).slice(0,16);
  const items=[],checks=[];
  for(let i=0;i<selected.length;i+=4)await Promise.all(selected.slice(i,i+4).map(async p=>{
    const key=`nfl:player-news:${p.playerId}`,hit=force?null:await cacheGet(env,key);
    let value=hit?.value;
    if(!value)try{
      const response=await fetch(`https://site.api.espn.com/apis/common/v3/sports/football/nfl/athletes/${p.playerId}/overview`,{signal:AbortSignal.timeout(5000)});
      if(!response.ok)throw new Error('Player news unavailable');
      const body=await response.json();value={items:normalizeNews({articles:body.news}).map(n=>({...n,playerIds:[String(p.playerId)]})),checkedAt:new Date().toISOString()};
      await cachePut(env,key,value,1800);
    }catch{value=(await cacheGet(env,key,true))?.value;}
    if(value){items.push(...value.items);checks.push({playerId:String(p.playerId),checkedAt:value.checkedAt});}
  }));
  return {items:[...new Map(items.map(n=>[n.url,n])).values()].sort((a,b)=>Date.parse(b.publishedAt)-Date.parse(a.publishedAt)).slice(0,30),checks,requested:selected.length};
}
