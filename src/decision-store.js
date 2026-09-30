import {sha256} from './security.js';
import {WAIVER_METHOD} from './waiver-plan.js';
import {encodeSnapshot} from './snapshot-codec.js';

// Stop archiving before snapshots crowd out operational data. Never evict history.
export const DECISION_STORAGE_BUDGET=128*1024*1024;

export async function freezeDecision(env,league,input,freshness){
  if(!input.waiversReady)return null;
  const currentWeek=input.settings.currentWeek;
  const relevant=[...input.roster,...input.freeAgents];
  // Post-kickoff snapshots remain useful for planning, but never enter a
  // prospective weekly accuracy claim.
  const prospective=!relevant.some(p=>p.game?.kickoff&&Date.parse(p.game.kickoff)<=input.settings.now);
  const inputs={roster:input.roster,freeAgents:input.freeAgents,settings:input.settings,freshness,prospective};
  // Identical inputs in the same six-hour window reuse their server-owned snapshot.
  // Changed inputs retain separate evidence; compression and a storage budget bound growth.
  const bucket=Math.floor(input.settings.now/21600000),mode=input.settings.mode;
  const {now,market,...stableSettings}=input.settings;
  const {verifiedAt,checkedAt,...stableMarket}=market||{};
  const id=await sha256(JSON.stringify({bucket,position:input.position||'',roster:input.roster,freeAgents:input.freeAgents,settings:stableSettings,market:stableMarket,method:WAIVER_METHOD}));
  const existing=await env.DB.prepare('SELECT id,created_at AS createdAt FROM decision_snapshots WHERE id=?').bind(id).first();
  if(existing)return {...existing,prospective,method:WAIVER_METHOD};
  const raw=JSON.stringify(inputs),hash=await sha256(raw),stored=await encodeSnapshot(raw),at=new Date().toISOString();
  // The budget test and insertion are one statement, so concurrent visits cannot overrun it.
  await env.DB.prepare('INSERT OR IGNORE INTO decision_snapshots (id,league_id,season,week,mode,method,created_at,inputs_json,input_hash) SELECT ?,?,?,?,?,?,?,?,? WHERE (SELECT COALESCE(SUM(length(CAST(inputs_json AS BLOB))),0) FROM decision_snapshots)+?<=?')
    .bind(id,String(league.leagueId),league.seasonYear,currentWeek,mode,WAIVER_METHOD,at,stored,hash,new TextEncoder().encode(stored).length,DECISION_STORAGE_BUDGET).run();
  const saved=await env.DB.prepare('SELECT id,created_at AS createdAt FROM decision_snapshots WHERE id=?')
    .bind(id).first();
  if(!saved){const error=new Error('Decision archive storage budget reached. Existing history is preserved.');error.code='SNAPSHOT_STORAGE_BUDGET';throw error;}
  return {...saved,prospective,method:WAIVER_METHOD};
}

export async function decisionHistory(env,league){
  const rows=await env.DB.prepare('SELECT id,week,mode,method,created_at AS createdAt,outcome_json AS outcomeJson,result_json AS feedbackJson FROM decision_snapshots WHERE league_id=? AND season=? ORDER BY created_at DESC LIMIT 20')
    .bind(String(league.leagueId),league.seasonYear).all();
  return (rows.results||[]).map(({outcomeJson,feedbackJson,...r})=>({...r,outcome:outcomeJson?JSON.parse(outcomeJson):null,feedback:feedbackJson?JSON.parse(feedbackJson):null}));
}
