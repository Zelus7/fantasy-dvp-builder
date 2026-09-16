import {sha256} from './security.js';
import {WAIVER_METHOD} from './waiver-plan.js';

export async function freezeDecision(env,league,input,freshness){
  if(!input.waiversReady)return null;
  const currentWeek=input.settings.currentWeek;
  const relevant=[...input.roster,...input.freeAgents];
  // Post-kickoff snapshots remain useful for planning, but never enter a
  // prospective weekly accuracy claim.
  const prospective=!relevant.some(p=>p.game?.kickoff&&Date.parse(p.game.kickoff)<=input.settings.now);
  const inputs={roster:input.roster,freeAgents:input.freeAgents,settings:input.settings,freshness,prospective};
  // One server-owned input snapshot per mode/position/6-hour window. This bounds
  // writes and retains the exact sources used, rather than accepting client summaries.
  const bucket=Math.floor(input.settings.now/21600000),mode=input.settings.mode;
  const {now,market,...stableSettings}=input.settings;
  const {verifiedAt,checkedAt,...stableMarket}=market||{};
  const id=await sha256(JSON.stringify({bucket,position:input.position||'',roster:input.roster,freeAgents:input.freeAgents,settings:stableSettings,market:stableMarket,method:WAIVER_METHOD}));
  const hash=await sha256(JSON.stringify(inputs)),at=new Date().toISOString();
  await env.DB.prepare('INSERT OR IGNORE INTO decision_snapshots (id,league_id,season,week,mode,method,created_at,inputs_json,input_hash) VALUES (?,?,?,?,?,?,?,?,?)')
    .bind(id,String(league.leagueId),league.seasonYear,currentWeek,mode,WAIVER_METHOD,at,JSON.stringify(inputs),hash).run();
  const saved=await env.DB.prepare('SELECT id,created_at AS createdAt FROM decision_snapshots WHERE id=?')
    .bind(id).first();
  return {...saved,prospective,method:WAIVER_METHOD};
}

export async function decisionHistory(env,league){
  const rows=await env.DB.prepare('SELECT id,week,mode,method,created_at AS createdAt,outcome_json AS outcomeJson,result_json AS feedbackJson FROM decision_snapshots WHERE league_id=? AND season=? ORDER BY created_at DESC LIMIT 20')
    .bind(String(league.leagueId),league.seasonYear).all();
  return (rows.results||[]).map(({outcomeJson,feedbackJson,...r})=>({...r,outcome:outcomeJson?JSON.parse(outcomeJson):null,feedback:feedbackJson?JSON.parse(feedbackJson):null}));
}
