import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {DatabaseSync} from 'node:sqlite';
import {claimPlanSummary,validateClaimPlan} from '../src/claim-plan.js';
import {readClaimPlan,saveClaimPlan} from '../src/claim-plan-store.js';

const now=Date.parse('2026-09-23T04:00:00Z');
const player=(id,position='WR')=>({playerId:id,name:`Player ${id}`,position,proTeam:'BUF',status:'WAIVERS',lineupSlotId:20});
const context={week:3,liveWeek:3,verifiedAt:new Date(now).toISOString(),market:{type:'FAAB',remaining:250,minimumBid:1},rosterCapacity:3,roster:[player('1','TE'),player('2'),player('3','D/ST')],freeAgents:[player('11'),player('12'),player('13'),player('14','D/ST')],kickoffs:{BUF:'2026-09-27T17:00:00Z'}};
const input={week:3,spendingLimit:48,claims:[{addId:'11',dropId:'1',bid:31},{addId:'12',dropId:'2',bid:16},{addId:'13',dropId:'1',bid:7},{addId:'14',dropId:'3',bid:1}]};
test('shared drops are mutually exclusive; different drops can succeed together',()=>{
  const result=validateClaimPlan(input,context,now);assert.equal(result.valid,true);assert.equal(result.maximumSpend,48);assert.equal(result.remainingAfterMaximum,202);assert.equal(result.paths.length,3);
  const revised=structuredClone(input);revised.spendingLimit=64;revised.claims[0].bid=41;revised.claims[1].bid=22;
  assert.equal(validateClaimPlan(revised,context,now).maximumSpend,64);
  assert.equal(claimPlanSummary([]).maximumSpend,0);
});
for(const [name,changes,pattern] of [
  ['insufficient limit',{spendingLimit:40},/exceeds/],['duplicate target',{claims:[input.claims[0],input.claims[0]]},/unique/],
  ['fractional bid',{claims:[{...input.claims[0],bid:1.1}]},/whole dollar/],['missing target',{claims:[{...input.claims[0],addId:'999'}]},/not confirmed/],
  ['missing drop',{claims:[{...input.claims[0],dropId:'999'}]},/drop is missing/],['no slot',{claims:[{...input.claims[0],dropId:null}]},/open roster/],
  ['string dollars',{claims:[{...input.claims[0],bid:'31'}]},/whole dollar/],['old week',{week:2},/live week/]
])test(`plan rejects ${name}`,()=>{const r=validateClaimPlan({...input,...changes},context,now);assert.equal(r.valid,false);assert.match(r.errors.join(' '),pattern);});
test('freshness, protected players, locks, minimum and known budget are mandatory',()=>{
  for(const changes of [{stale:true},{verifiedAt:'bad'},{verifiedAt:new Date(now-300001).toISOString()},{verifiedAt:new Date(now+1).toISOString()},{protectedIds:['1']},{kickoffs:{}},{kickoffs:{BUF:'2026-09-20'}},{market:{...context.market,minimumBid:2}},{market:{...context.market,remaining:null}}])assert.equal(validateClaimPlan(input,{...context,...changes},now).valid,false);
});
test('available roster presence and self-report never turn into a verified receipt',()=>{
  const r=validateClaimPlan({...input,claims:[{...input.claims[0],reportedPending:true}]},context,now);assert.match(r.warnings.join(' '),/not independently verified/);assert.equal(r.claims[0].status,undefined);
});
test('open slots are independent spend, and injured drops require an explicit warning',()=>{
  const r=validateClaimPlan({week:3,spendingLimit:20,claims:[{addId:'11',dropId:null,bid:4},{addId:'12',dropId:'1',bid:8}]},{...context,rosterCapacity:4,roster:[{...player('1'),injuryStatus:'IR'},player('2'),player('3')]},now);
  assert.equal(r.valid,true);assert.equal(r.maximumSpend,12);assert.match(r.warnings.join(' '),/future return value/);
});
test('claim plan persistence uses optimistic versions and separates teams',async t=>{
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());db.exec(readFileSync(new URL('../migrations/0004_claim_plans.sql',import.meta.url),'utf8'));
  const env={DB:{prepare(sql){const make=(args=[])=>({bind(...v){return make(v)},async first(){return db.prepare(sql).get(...args)||null},async run(){return {meta:{changes:Number(db.prepare(sql).run(...args).changes)}}}});return make();}}};
  const league={leagueId:'fixture',teamId:'9',seasonYear:2026,currentWeek:3},review=validateClaimPlan(input,context,now);
  assert.equal((await readClaimPlan(env,league)).version,0);
  assert.equal((await saveClaimPlan(env,league,0,review)).version,1);
  await assert.rejects(saveClaimPlan(env,league,0,review),{code:'PLAN_CHANGED'});
  assert.equal((await saveClaimPlan(env,league,1,review)).version,2);
  await assert.rejects(saveClaimPlan(env,league,1,review),{code:'PLAN_CHANGED'});
  assert.equal((await readClaimPlan(env,{...league,teamId:'other'})).version,0);
  assert.equal((await readClaimPlan(env,league)).claims[0].bid,31);
});
