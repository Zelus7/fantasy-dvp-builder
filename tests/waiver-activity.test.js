import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {normalizeActivity,recordActivity,readActivity,syncActivity,activitySummary} from '../src/waiver-activity.js';
import {historicalBidContext} from '../src/waiver-market.js';
import {activityHtml} from '../public/waiver-activity-ui.js';
import {fetchWaiverActivity} from '../src/espn.js';
import {saveCredentials} from '../src/db.js';
const league={leagueId:'1',seasonYear:2026,teamId:'9',currentWeek:3};
const tx={id:'claim-1',teamId:9,scoringPeriodId:3,type:'WAIVER',status:'EXECUTED',isPending:false,bidAmount:31,processDate:Date.now()-1000,items:[{type:'ADD',playerId:21},{type:'DROP',playerId:10}]};
const body=(transactions)=>({id:1,seasonId:2026,transactions});
function database(t){
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());
  for(const file of ['0001_initial.sql','0006_espn_claim_observations.sql'])db.exec(readFileSync(new URL(`../migrations/${file}`,import.meta.url),'utf8'));
  const env={DB:{prepare(sql){const make=(args=[])=>({bind(...a){return make(a)},async first(){return db.prepare(sql).get(...args)||null},async all(){return {results:db.prepare(sql).all(...args)}},async run(){return db.prepare(sql).run(...args)}});return make()},async batch(statements){return Promise.all(statements.map(s=>s.run()))}}};
  return {db,env};
}
test('missing feed and mismatched scope cannot confirm empty activity',()=>{
  for(const value of [{id:1,seasonId:2026},body(null),{...body([]),id:2},{...body([]),seasonId:2025}])assert.equal(normalizeActivity(value,league,{week:3}).status,'unavailable');
  assert.equal(normalizeActivity(body([]),league,{week:3}).status,'fresh');
});
test('completed offers use processDate, exact week, whole bids and explicit terminal outcomes',()=>{
  const variants=[tx,{...tx,id:2,status:'PENDING'}, {...tx,id:3,processDate:undefined,executionDate:tx.processDate},{...tx,id:4,bidAmount:null},{...tx,id:5,scoringPeriodId:2},{...tx,id:6,bidAmount:1.5},{...tx,id:7,isPending:true},{...tx,id:8,processDate:Date.now()+9999999},{...tx,id:9,status:'MYSTERY'},null];
  const r=normalizeActivity(body(variants),league,{week:3});assert.equal(r.rows.length,1);assert.equal(r.rows[0].paid,31);assert.equal(r.status,'partial');
  const skipped=normalizeActivity(body([{...tx,status:'FAILED_PLAYERALREADYDROPPED'}]),league,{week:3}).rows[0];assert.equal(skipped.status,'skipped');assert.equal(skipped.paid,0);
  assert.equal(normalizeActivity(body([{...tx,status:'FAILED_ROSTERLIMIT'}]),league,{week:3}).rows[0].status,'unsuccessful');
});
test('pending feed reveals only explicitly pending own-team claims; stale processed PENDING is ambiguous',()=>{
  const p={...tx,status:'PENDING',isPending:true,processDate:undefined};
  const r=normalizeActivity({...body([]),pendingTransactions:[p,{...p,id:2,teamId:8},{...p,id:3,isPending:false}]},league,{pending:true});
  assert.equal(r.rows.length,1);assert.equal(r.rows[0].teamId,'9');assert.equal(r.rows[0].paid,null);assert.equal(r.status,'partial');
  assert.equal(normalizeActivity(body([p]),league,{week:3}).rows.length,0);
});
test('duplicate and conflicting IDs never inflate results or remove unrelated rows',()=>{
  const r=normalizeActivity(body([tx,{...tx,id:'other'},tx,{...tx,bidAmount:12},{...tx,bidAmount:13}]),league,{week:3});
  assert.deepEqual(r.rows.map(r=>r.id),['other']);assert.equal(r.status,'partial');
});
test('immutable archive deduplicates retries and excludes changed terminal evidence',async t=>{
  const {db,env}=database(t),rows=normalizeActivity(body([tx]),league,{week:3}).rows;
  await recordActivity(env,league,rows);await recordActivity(env,league,rows);assert.equal((await readActivity(env,league)).rows.length,1);
  await recordActivity(env,league,[{...rows[0],bid:32,paid:32}]);const result=await readActivity(env,league);assert.equal(result.conflicts,1);assert.equal(result.rows.length,0);
  assert.throws(()=>db.exec('DELETE FROM espn_claim_observations'),/immutable/);
  assert.equal((await readActivity(env,{...league,leagueId:2})).rows.length,0);
});
test('read-only sync scopes weeks, caches checks and preserves stale pending evidence',async t=>{
  const {env}=database(t);let reads=0,failed=false;
  const fetchPage=async(pending,week)=>{reads++;if(failed)throw new Error('private upstream detail');return pending?{...body([]),pendingTransactions:[{...tx,status:'PENDING',isPending:true,processDate:undefined}]}:body(week===3?[tx]:[]);};
  const first=await syncActivity(env,league,fetchPage);assert.equal(first.processed.status,'fresh');assert.equal(first.pending.rows.length,1);assert.equal(reads,4);
  await syncActivity(env,league,fetchPage);assert.equal(reads,4);
  failed=true;const stale=await syncActivity(env,league,fetchPage,{force:true});assert.equal(stale.pending.status,'stale');assert.equal(stale.pending.checkedAt,first.pending.checkedAt);assert.equal(stale.processed.status,'unavailable');assert.equal((await readActivity(env,league)).rows.length,1);assert.ok(!JSON.stringify(stale).includes('private upstream'));
});
test('plan reconciliation requires exact bid, drop, week and a unique later transaction',()=>{
  const row=normalizeActivity(body([tx]),league,{week:3}).rows[0],plan={week:3,updatedAt:new Date(tx.processDate-1000).toISOString(),claims:[{addId:'21',dropId:'10',addName:'Target',bid:31}]};
  const activity={checkedAt:new Date().toISOString(),pending:{status:'fresh',rows:[]},processed:{status:'fresh'}},cache={stale:false,updatedAt:new Date().toISOString()};
  const summary=rows=>activitySummary(activity,{rows,conflicts:0},league,[{playerId:'21',name:'Target',position:'WR'}],[],plan,[{playerId:'21'}],cache);
  assert.equal(summary([row]).claims[0].status,'won');assert.equal(summary([row]).recentSpend,31);
  for(const bad of [{...row,bid:30},{...row,dropId:'other'},{...row,week:2},{...row,at:plan.updatedAt.replace('2026','2025')}])assert.equal(summary([bad]).claims[0].status,'unresolved');
  assert.equal(summary([row,{...row,id:'duplicate'}]).claims[0].status,'unresolved');
});
test('failed offers remain descriptive, never a success rate or minimum winning price',()=>{
  const at=new Date().toISOString(),history=historicalBidContext('WR',{winningBids:[{id:'won',position:'WR',at,amount:7}],failedOffers:[{id:'lost',position:'WR',at,status:'unavailable',bid:100},{id:'skipped',position:'WR',at,status:'skipped',bid:200}]});
  assert.equal(history.median,7);assert.equal(history.failedSampleSize,2);assert.match(history.explanation,/not all price losses/);
});
test('activity UI escapes ESPN labels and does not claim unavailable pending is empty',()=>{
  const html=activityHtml({checkedAt:null,pending:{status:'unavailable',rows:[]},processed:{status:'partial'},recentSpend:0,outcomes:[{playerName:'<script>bad</script>',status:'won',bid:1,paid:1}],claims:[],explanation:'partial'});
  assert.match(html,/No pending claims can be confirmed/);assert.ok(!html.includes('<script>'));assert.match(html,/&lt;script&gt;/);
});
test('a skipped shared-drop fallback does not create a false new roster need',()=>{
  const row=normalizeActivity(body([tx]),league,{week:3}).rows[0];
  const result=activitySummary({pending:{rows:[]},processed:{status:'fresh'}},{rows:[row,{...row,id:'fallback',playerId:'22',bid:7,paid:0,status:'skipped'}]},league,[{playerId:'21',name:'Primary'}],[],{week:3,updatedAt:new Date(tx.processDate-1000).toISOString(),claims:[{addId:'22',dropId:'10',bid:7}]},[{playerId:'21'}],{updatedAt:new Date().toISOString()});
  assert.match(result.claims[0].next,/Fallback not needed: Primary/);
});
test('adapter sends credentialed GETs only to fixed ESPN origin; off-origin redirects cannot receive cookies',async t=>{
  const {env}=database(t);env.CREDENTIAL_ENCRYPTION_KEY=Buffer.alloc(32,3).toString('base64');await saveCredentials(env,{swid:'fixture-owner',s2:'fixture-cookie'});
  let reads=0;t.mock.method(globalThis,'fetch',async(url,options)=>{
    reads++;assert.equal(new URL(url).origin,'https://lm-api-reads.fantasy.espn.com');assert.equal(options.method,'GET');assert.equal(options.redirect,'manual');
    return new Response(null,{status:302,headers:{Location:'https://not-espn.example/'}});
  });
  const result=await fetchWaiverActivity(env,league);assert.equal(reads,4);assert.equal(result.pending.status,'unavailable');assert.equal(result.processed.status,'unavailable');assert.ok(!JSON.stringify(result).includes('fixture-cookie'));
});
test('oversized upstream data fails closed without persisting a partial receipt',async t=>{
  const {env}=database(t);env.CREDENTIAL_ENCRYPTION_KEY=Buffer.alloc(32,3).toString('base64');await saveCredentials(env,{swid:'fixture-owner',s2:'fixture-cookie'});
  t.mock.method(globalThis,'fetch',async()=>new Response('x'.repeat(2*1024*1024+1)));
  const result=await fetchWaiverActivity(env,league);assert.equal(result.processed.status,'unavailable');assert.equal((await readActivity(env,league)).rows.length,0);
});
