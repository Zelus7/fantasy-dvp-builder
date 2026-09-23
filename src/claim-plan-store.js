import {HttpError} from './http.js';

const key=league=>[String(league.leagueId),Number(league.seasonYear),String(league.teamId)];
export async function readClaimPlan(env,league){
  const row=await env.DB.prepare('SELECT version,plan_json AS planJson,updated_at AS updatedAt FROM claim_plans WHERE league_id=? AND season=? AND team_id=?').bind(...key(league)).first();
  return row?{version:row.version,...JSON.parse(row.planJson),updatedAt:row.updatedAt}:{version:0,claims:[],spendingLimit:0,week:league.currentWeek,updatedAt:null};
}
export async function saveClaimPlan(env,league,version,review){
  if(!Number.isSafeInteger(version)||version<0)throw new HttpError(400,'INVALID_VERSION','Reload the saved plan before editing.');
  const plan={week:review.week,claims:review.claims,spendingLimit:review.spendingLimit},at=new Date().toISOString();
  const result=version===0?
    await env.DB.prepare('INSERT OR IGNORE INTO claim_plans (league_id,season,team_id,version,plan_json,updated_at) VALUES (?,?,?,1,?,?)').bind(...key(league),JSON.stringify(plan),at).run():
    await env.DB.prepare('UPDATE claim_plans SET version=version+1,plan_json=?,updated_at=? WHERE league_id=? AND season=? AND team_id=? AND version=?').bind(JSON.stringify(plan),at,...key(league),version).run();
  if(Number(result.meta?.changes)!==1)throw new HttpError(409,'PLAN_CHANGED','This plan changed in another tab. Reload it before saving; your edit was not applied.');
  return {version:version+1,...plan,updatedAt:at};
}
