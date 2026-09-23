import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {parseOfferReport,reviewOfferReport,reconcileClaimPlan} from '../src/claim-outcomes.js';
import {readClaimOutcomes,saveClaimReceipt} from '../src/claim-outcome-store.js';
import {saveClaimPlan} from '../src/claim-plan-store.js';
const now=Date.parse('2026-09-23T10:00:00Z');
const plan={version:1,updatedAt:'2026-09-23T05:00:00Z',week:3,spendingLimit:24,claims:[
  {addId:'11',addName:'Fixture Alpha',dropId:'1',dropName:'Fixture Old',bid:20},
  {addId:'12',addName:'Fixture Beta',dropId:'1',dropName:'Fixture Old',bid:5},
  {addId:'13',addName:'Fixture Defense',dropId:'2',dropName:'Fixture Prior Defense',bid:4}]};
const text='Free Agent Offers Processed\nPOS\tTEAM\tCLAIM\tOFFER\tRESULTS\n1\tFixture Team\tFixture Alpha BUF, WR\t$20\tAdded.FX dropped Fixture Old, BUF WR to Waivers.\n\tOther Team\tFixture Alpha BUF, WR\t$3\tUnsuccessful. Reason: Player has already been added to another team.\n\tFixture Team\tFixture Beta BUF, WR\t$5\tUnsuccessful. Reason: A player involved has already been dropped\n2\tOther Team\tFixture Defense BUF, D/ST\t$6\tAdded.\n\tFixture Team\tFixture Defense BUF, D/ST\t$4\tUnsuccessful. Reason: Player has already been added to another team.';
const input={reportDate:'2026-09-23',text};
test('report matches exact team, name, bid and successful conditional drop',()=>{
  const r=reviewOfferReport(input,plan,'Fixture Team',now);assert.equal(r.totalPaid,20);assert.deepEqual(r.claims.map(c=>c.status),['won','skipped','outbid']);
  assert.equal(r.claims[0].highestOtherBid,3);assert.equal(r.claims[1].paid,0);assert.equal(r.claims[2].highestOtherBid,6);
  assert.match(r.source,/user-supplied/);assert.match(r.caveat,/hindsight/);
  assert.equal(parseOfferReport(text.replaceAll('\t','\n\t\n')).length,5);
});
for(const [name,change] of [
  ['wrong team',{text:text.replaceAll('Fixture Team','Wrong Team')}],['wrong bid',{text:text.replace('$20','$21')}],
  ['wrong drop',{text:text.replace('dropped Fixture Old','dropped Someone Else')}],['future date',{reportDate:'2026-09-24'}],
  ['prior date',{reportDate:'2026-09-22'}],['invalid date',{reportDate:'2026-02-30'}],['oversize',{text:'x'.repeat(24001)}],
  ['duplicate',{text:text+'\nFixture Team\nFixture Alpha BUF, WR\n$20\nAdded.FX dropped Fixture Old, BUF WR to Waivers.'}],
  ['partial',{text:text.slice(0,text.indexOf('\n\tFixture Team\tFixture Beta'))}],
  ['double conditional win',{text:text.replace('Unsuccessful. Reason: A player involved has already been dropped','Added.FX dropped Fixture Old, BUF WR to Waivers.')}]
])test(`reject ${name} report`,()=>assert.throws(()=>reviewOfferReport({...input,...change},plan,'Fixture Team',now)));
test('ownership and budget cannot manufacture an outcome, price, or pending state',()=>{
  let r=reconcileClaimPlan(plan,[{playerId:'11'}],{updatedAt:new Date(now).toISOString()},null,now);
  assert.equal(r.claims[0].onRoster,true);assert.equal(r.claims[0].outcome,'unresolved');assert.equal(r.claims[0].paid,undefined);
  assert.equal(r.claims[1].outcome,'unresolved');
  for(const cache of [{stale:true,updatedAt:new Date(now).toISOString()},{updatedAt:'bad'},{updatedAt:new Date(now+1).toISOString()},{updatedAt:new Date(now-300001).toISOString()}])assert.equal(reconcileClaimPlan(plan,[],cache,null,now).claims[0].onRoster,null);
  r=reconcileClaimPlan(plan,[],{updatedAt:new Date(now).toISOString()},reviewOfferReport(input,plan,'Fixture Team',now),now);
  assert.equal(r.claims[0].discrepancy,true);assert.equal(r.claims[1].discrepancy,false);
});
test('archives migrate existing plans and preserve revisions, team isolation and immutable idempotent receipts',async t=>{
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());
  db.exec(readFileSync(new URL('../migrations/0004_claim_plans.sql',import.meta.url),'utf8'));
  const env={DB:{prepare(sql){const make=(a=[])=>({bind(...v){return make(v)},async first(){return db.prepare(sql).get(...a)||null},async all(){return {results:db.prepare(sql).all(...a)}},async run(){return {meta:{changes:Number(db.prepare(sql).run(...a).changes)}}}});return make()}}};
  const league={leagueId:'fixture',teamId:'9',seasonYear:2026};
  await saveClaimPlan(env,league,0,plan);
  db.exec(readFileSync(new URL('../migrations/0005_claim_outcomes.sql',import.meta.url),'utf8'));
  const receipt=reviewOfferReport(input,plan,'Fixture Team',now);
  await saveClaimReceipt(env,league,1,receipt);await saveClaimReceipt(env,league,1,receipt);
  await assert.rejects(saveClaimReceipt(env,league,1,{...receipt,totalPaid:0}),{code:'RECEIPT_CONFLICT'});
  await saveClaimPlan(env,league,1,{...plan,claims:[]});
  const h=await readClaimOutcomes(env,league);assert.equal(h.length,2);assert.equal(h[0].plan.claims.length,0);assert.equal(h[1].receipt.totalPaid,20);assert.equal(h[1].plan.claims.length,3);
  assert.deepEqual(await readClaimOutcomes(env,{...league,teamId:'2'}),[]);
  await assert.rejects(saveClaimReceipt(env,{...league,teamId:'2'},1,receipt),{code:'RECEIPT_CONFLICT'});
  await assert.rejects(saveClaimPlan(env,league,1,plan),{code:'PLAN_CHANGED'});
  assert.equal((await readClaimOutcomes(env,league)).length,2);
});
