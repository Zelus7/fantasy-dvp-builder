import {HttpError} from './http.js';
const key=league=>[String(league.leagueId),Number(league.seasonYear),String(league.teamId)];
export async function readClaimOutcomes(env,league){
  const {results}=await env.DB.prepare('SELECT p.version,p.plan_json AS planJson,p.updated_at AS updatedAt,r.receipt_json AS receiptJson,r.recorded_at AS recordedAt FROM claim_plan_versions p LEFT JOIN claim_receipts r USING (league_id,season,team_id,version) WHERE p.league_id=? AND p.season=? AND p.team_id=? ORDER BY p.version DESC LIMIT 12').bind(...key(league)).all();
  return results.map(r=>({plan:{...JSON.parse(r.planJson),version:r.version,updatedAt:r.updatedAt},receipt:r.receiptJson?{...JSON.parse(r.receiptJson),recordedAt:r.recordedAt}:null}));
}
export async function saveClaimReceipt(env,league,version,receipt){
  const body=JSON.stringify(receipt);
  const result=await env.DB.prepare('INSERT OR IGNORE INTO claim_receipts (league_id,season,team_id,version,receipt_json,recorded_at) SELECT league_id,season,team_id,version,?,? FROM claim_plans WHERE league_id=? AND season=? AND team_id=? AND version=?').bind(body,new Date().toISOString(),...key(league),version).run();
  if(Number(result.meta?.changes)===1)return;
  const row=await env.DB.prepare('SELECT receipt_json AS receiptJson FROM claim_receipts WHERE league_id=? AND season=? AND team_id=? AND version=?').bind(...key(league),version).first();
  if(row?.receiptJson===body)return; // An identical retry is harmless; receipts are immutable.
  throw new HttpError(409,'RECEIPT_CONFLICT','The plan changed or already has a different saved report. Reload; no report was overwritten.');
}
