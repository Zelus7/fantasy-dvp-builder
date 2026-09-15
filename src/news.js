import {cacheGet,cachePut} from './db.js';
const KEY='nfl:news';
import {normalizeNews} from './news-format.js';
export {normalizeNews};
export async function fetchNews(env){
  const hit=await cacheGet(env,KEY);if(hit)return hit.value;
  try{
    const response=await fetch('https://site.api.espn.com/apis/site/v2/sports/football/nfl/news?limit=12',{signal:AbortSignal.timeout(5000)});
    if(!response.ok)throw new Error('News unavailable');
    const items=normalizeNews(await response.json());if(!items.length)throw new Error('Empty news feed');
    const value={generatedAt:new Date().toISOString(),items};await cachePut(env,KEY,value,1800);return value;
  }catch{return (await cacheGet(env,KEY,true))?.value||null}
}
