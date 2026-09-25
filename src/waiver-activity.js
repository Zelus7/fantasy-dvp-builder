// Read-only ESPN evidence. An absent feed, a changed roster, or a stale PENDING
// record in the processed feed never proves a claim's outcome.
import {cacheGet,cachePut} from './db.js';
import {sha256} from './security.js';

const id=value=>typeof value==='number'&&Number.isSafeInteger(value)?String(value):typeof value==='string'&&/^[\w{}:-]{1,100}$/.test(value)?value:null;
const money=value=>typeof value==='number'&&Number.isSafeInteger(value)&&value>=0&&value<=100000?value:null;
const terminal=new Map([
  ['EXECUTED',['won','Added by ESPN.']],
  ['FAILED_INVALIDPLAYERSOURCE',['unavailable','The target was no longer available. This does not establish the minimum winning bid.']],
  ['FAILED_PLAYERALREADYDROPPED',['skipped','The conditional drop was already used.']],
  ['FAILED_AUCTIONBUDGETEXCEEDED',['unsuccessful','Insufficient FAAB when processed.']],
  ['FAILED_POSITIONLIMIT',['unsuccessful','Position limit prevented the claim.']],
  ['FAILED_ROSTERLIMIT',['unsuccessful','Roster limit prevented the claim.']],
  ['FAILED_ROSTERLOCK',['unsuccessful','Roster lock prevented the claim.']],
]);
const at=value=>typeof value==='number'&&Number.isFinite(value)&&value>=946684800000&&value<=Date.now()+300000?new Date(value).toISOString():null;
const scope=(body,league)=>String(body?.id)===String(league.leagueId)&&Number(body?.seasonId)===Number(league.seasonYear);
const failure=error=>({upstreamStatus:Number.isInteger(error?.upstreamStatus)?error.upstreamStatus:null,stage:['network','json'].includes(error?.activityStage)?error.activityStage:'read'});

export function normalizeActivity(body,league,{pending=false,week}={}){
  const field=pending?'pendingTransactions':'transactions';
  if(!scope(body,league)||!Array.isArray(body[field]))return {status:'unavailable',rows:[],ignored:0,reason:'ESPN did not return an explicit, league-matched transaction list. Absence is not proof of no claims.',shape:{leagueMatched:String(body?.id)===String(league.leagueId),seasonMatched:Number(body?.seasonId)===Number(league.seasonYear),listPresent:Array.isArray(body?.[field])}};
  const rows=[],seen=new Map(),unclassified={};let ignored=0;
  for(const tx of body[field].slice(0,500)){
    if(!tx||!['WAIVER','WAIVER_ERROR','WAIVER_BID'].includes(tx.type))continue;
    // Never retain or expose another manager's unprocessed offer.
    if(pending&&String(tx.teamId)!==String(league.teamId))continue;
    const items=Array.isArray(tx.items)?tx.items:[],adds=items.filter(i=>i?.type==='ADD'),drops=items.filter(i=>i?.type==='DROP');
    const result=terminal.get(tx.status),processedAt=at(tx.processDate);
    if(!id(tx.id)||!id(tx.teamId)||money(tx.bidAmount)===null||adds.length!==1||drops.length>1||!id(adds[0]?.playerId)||(drops.length&&!id(drops[0]?.playerId))||
      (pending?(tx.isPending!==true||tx.status!=='PENDING'||processedAt!==null):(!result||!processedAt||tx.isPending===true))||
      !Number.isInteger(tx.scoringPeriodId)||tx.scoringPeriodId<0||tx.scoringPeriodId>18||(!pending&&tx.scoringPeriodId!==week)){
      ignored++;const code=typeof tx.status==='string'&&/^[A-Z_]{1,60}$/.test(tx.status)?tx.status:'UNKNOWN';unclassified[code]=(unclassified[code]||0)+1;continue;
    }
    const row={id:id(tx.id),teamId:id(tx.teamId),week:tx.scoringPeriodId,playerId:id(adds[0].playerId),dropId:drops.length?id(drops[0].playerId):null,
      bid:tx.bidAmount,paid:pending?null:result[0]==='won'?tx.bidAmount:0,status:pending?'pending':result[0],reason:pending?'ESPN currently lists this claim as pending.':result[1],at:pending?null:processedAt,source:'ESPN read API'};
    const serialized=JSON.stringify(row);
    if(seen.has(row.id)){if(seen.get(row.id)!==serialized){ignored++;const index=rows.findIndex(r=>r.id===row.id);if(index>=0)rows.splice(index,1);seen.set(row.id,null);}continue;}
    seen.set(row.id,serialized);rows.push(row);
  }
  const partial=ignored>0||body[field].length>=500;
  return {status:partial?'partial':'fresh',rows,ignored,unclassified,reason:partial?'Some records were ambiguous or the 500-record limit was reached. Coverage is incomplete.':'Explicit ESPN transaction list returned.'};
}

export async function recordActivity(env,league,rows){
  const statements=[];
  for(const row of rows){
    const text=JSON.stringify(row),hash=await sha256(text);
    statements.push(env.DB.prepare('INSERT OR IGNORE INTO espn_claim_observations (league_id,season,transaction_id,fingerprint,outcome_json,processed_at) VALUES (?,?,?,?,?,?)').bind(String(league.leagueId),Number(league.seasonYear),row.id,hash,text,row.at));
  }
  for(let i=0;i<statements.length;i+=50)await env.DB.batch(statements.slice(i,i+50));
}

export async function readActivity(env,league){
  const args=[String(league.leagueId),Number(league.seasonYear)];
  const {results}=await env.DB.prepare('SELECT transaction_id,MIN(outcome_json) AS payload,COUNT(*) AS variants FROM espn_claim_observations WHERE league_id=? AND season=? GROUP BY transaction_id ORDER BY MAX(processed_at) DESC LIMIT 1500').bind(...args).all();
  return {rows:results.filter(r=>r.variants===1).map(r=>JSON.parse(r.payload)),conflicts:results.filter(r=>r.variants!==1).length,truncated:results.length>=1500};
}

// fetchPage is supplied by espn.js; only its fixed-origin GET adapter has credentials.
export async function syncActivity(env,league,fetchPage,{force=false}={}){
  const week=Number(league.liveWeek||league.currentWeek),key=`espn:activity:v1:${league.leagueId}:${league.seasonYear}:${league.teamId}:${week}`;
  const hit=force?null:await cacheGet(env,key);if(hit)return hit.value;
  if(!Number.isInteger(week)||week<1||week>18)return {checkedAt:null,processed:{status:'unavailable',weeks:[]},pending:{status:'unavailable',rows:[]},reason:'Current scoring week unavailable.'};
  const checkedAt=new Date().toISOString(),weeks=Array.from({length:Math.min(3,week)},(_,i)=>week-i);
  const pages=await Promise.all([...weeks.map(async period=>{
    try{return {week:period,...normalizeActivity(await fetchPage(false,period),league,{week:period})};}
    catch(error){return {week:period,status:'unavailable',rows:[],ignored:0,reason:'ESPN processed-offer refresh failed.',...failure(error)};}
  }), (async()=>{try{return normalizeActivity(await fetchPage(true,week),league,{pending:true});}catch(error){return {status:'unavailable',rows:[],reason:'ESPN pending-claim refresh failed.',...failure(error)};}})()]);
  const pending=pages.pop();
  await recordActivity(env,league,pages.flatMap(p=>p.rows));
  const previous=(await cacheGet(env,key,true))?.value;
  // Keep a failed pending read visibly stale, never replace it with a false empty list.
  if(pending.status==='unavailable'&&previous?.pending?.checkedAt){pending.rows=previous.pending.rows;pending.status='stale';pending.checkedAt=previous.pending.checkedAt;}
  else pending.checkedAt=pending.status==='unavailable'?null:checkedAt;
  const value={checkedAt,pending,processed:{status:pages.every(p=>p.status==='fresh')?'fresh':pages.every(p=>p.status==='unavailable')?'unavailable':'partial',weeks:pages.map(({week,status,ignored,reason,shape,upstreamStatus,stage,unclassified})=>({week,status,ignored,reason,shape,upstreamStatus,stage,unclassified}))}};
  await cachePut(env,key,value,180);return value;
}

export function enrichActivity(rows,players,teams=[]){
  const byId=new Map(players.map(p=>[String(p.playerId),p])),names=new Map(teams.map(t=>[String(t.id),t.name]));
  return rows.map(r=>({...r,playerName:byId.get(r.playerId)?.name||`Player ${r.playerId}`,position:byId.get(r.playerId)?.position||null,dropName:r.dropId?(byId.get(r.dropId)?.name||`Player ${r.dropId}`):null,teamName:names.get(r.teamId)||`Team ${r.teamId}`}));
}

export function activitySummary(activity,archive,league,players,teams,plan,roster,rosterCache){
  const rows=enrichActivity(archive.rows,players,teams),own=rows.filter(r=>r.teamId===String(league.teamId)),pending=enrichActivity(activity.pending.rows||[],players,teams);
  const rosterFresh=!rosterCache?.stale&&Date.now()-Date.parse(rosterCache?.updatedAt)<300000;
  const claims=(plan.claims||[]).map(c=>{
    const matches=own.filter(r=>r.playerId===c.addId&&r.dropId===(c.dropId||null)&&r.bid===c.bid&&r.week===plan.week&&Date.parse(r.at)>=Date.parse(plan.updatedAt));
    const receipt=matches.length===1?matches[0]:null,onRoster=rosterFresh?roster.some(p=>String(p.playerId)===c.addId):null;
    const primary=receipt?.status==='skipped'&&own.find(r=>r.status==='won'&&r.dropId&&r.dropId===c.dropId&&r.week===plan.week&&Date.parse(r.at)>=Date.parse(plan.updatedAt));
    const dropGone=rosterFresh&&c.dropId&&!roster.some(p=>String(p.playerId)===c.dropId);
    return {addId:c.addId,addName:c.addName,receipt,onRoster,status:receipt?.status||'unresolved',next:matches.length>1?'Multiple matching ESPN transactions; review the report.':receipt?.status==='won'?(onRoster===true?'Acquired. Recheck your best lineup.':'ESPN recorded a win; the current roster may reflect a later move.'):primary?`Fallback not needed: ${primary.playerName} won using the same conditional drop. Do not repeat the fallback automatically.`:receipt?(dropGone?'The original drop is no longer on your roster. Reassess current waiver options; do not repeat this old add/drop.':onRoster===false?'Target missed. Reassess this position and remaining free agents.':'Recheck the live roster before choosing a replacement.'):'No unique processed result matched this saved plan. Do not assume it was submitted.'};
  });
  return {...activity,pending:{...activity.pending,rows:pending},outcomes:own.slice(0,40),claims,conflicts:archive.conflicts,
    recentSpend:own.filter(r=>r.week===Number(league.liveWeek||league.currentWeek)).reduce((n,r)=>n+r.paid,0),
    explanation:'Direct read-only ESPN records, kept separately from pasted reports. Spend is the sum of observed completed wins this scoring week, not a full budget audit. Unmatched plans and incomplete feeds remain unconfirmed.'};
}
