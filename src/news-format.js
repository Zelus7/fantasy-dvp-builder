export function normalizeNews(body){
  return (Array.isArray(body?.articles)?body.articles:[]).filter(a=>typeof a.headline==='string'&&/^https:\/\/([a-z0-9-]+\.)?espn\.com\//i.test(a.links?.web?.href||'')&&Number.isFinite(Date.parse(a.published))).slice(0,12).map(a=>({title:a.headline.slice(0,240),url:a.links.web.href,publishedAt:a.published,source:'ESPN',playerIds:(a.categories||[]).filter(c=>c.type==='athlete'&&c.athleteId).map(c=>String(c.athleteId))}));
}
export function rankNews(items,players){
  const ids=new Set(players.map(p=>String(p.playerId))),names=players.map(p=>String(p.name||'').replace(/\s+(Jr\.|Sr\.|II|III)$/,'').toLowerCase()).filter(n=>n.length>5);
  const relevance=item=>(item.playerIds||[]).some(id=>ids.has(String(id)))||names.some(name=>item.title.toLowerCase().includes(name));
  return items.map(item=>({...item,rosterMention:relevance(item)})).sort((a,b)=>Number(b.rosterMention)-Number(a.rosterMention)||Date.parse(b.publishedAt)-Date.parse(a.publishedAt));
}
