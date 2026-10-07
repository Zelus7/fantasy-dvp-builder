import test from 'node:test';
import assert from 'node:assert/strict';
import {planWaivers,bidGuidance} from '../src/waiver-plan.js';
import {currentKicking,validateKicking,reviewWaiverMove,waiverReviewCoverage} from '../src/waiver-review.js';
import {recommendationDraftBid,reviewDetails} from '../public/waiver-ui.js';
import {actionPlanDetails} from '../public/planning-ui.js';
import {evaluateFrozenDecision} from '../src/decision-outcomes.js';
import {WAIVER_METHOD} from '../src/waiver-plan.js';
const now=Date.parse('2026-10-07T12:00:00Z');
const generatedAt=new Date(now).toISOString();
const player=(playerId,position,points,extra={})=>({playerId,name:playerId,position,team:'IND',proTeam:'IND',median:points,projectedPoints:points,seasonProjectedPoints:points*17,injuryStatus:'ACTIVE',status:'FREEAGENT',eligibleSlotIds:[{QB:0,RB:2,WR:4,K:17}[position]],lineupSlotId:20,isAvailable:true,eligibleForRecommendation:true,hasEstimate:true,game:{kickoff:'2026-10-11T17:00:00Z'},...extra});
const kicker=(id,points,attempts)=>player(id,'K',points,{feature:{season:2026,generatedAt,dataThroughWeek:4,opportunity:{contextOnly:true,kicking:{season:2026,team:'IND',generatedAt,throughWeek:4,datasetThroughWeek:4,games:4,attempts:attempts.reduce((s,n)=>s+n,0),made:attempts.reduce((s,n)=>s+n,0),patMade:8,patAttempts:8,longMade:2,longAttempts:2,unknownDistances:0,weekly:attempts.map((attempts,i)=>({week:i+1,attempts})),source:'fixture'}}}});
const history=(points,extra={})=>({season:2026,generatedAt,dataThroughWeek:4,currentGames:4,seasonPpg:points.reduce((s,n)=>s+n,0)/points.length,opportunity:{games:4,throughWeek:4,weeklyPoints:points.map((points,i)=>({week:i+1,points})),weeklyUsage:points.map((_,i)=>({week:i+1,team:'IND',carries:11,targets:1})),...extra}});
const options=players=>({season:2026,currentWeek:5,endWeek:5,now,mode:'week',slots:{17:1},rosterCapacity:1,schedules:Object.fromEntries(players.map(p=>[p.playerId,{weeks:[{week:5,bye:false,missingSchedule:false}]}])),market:{type:'FAAB',budget:250,remaining:200,minimumBid:0,verifiedAt:generatedAt}});
test('Shrader/Myers shape: near-tied projection with repeated FG opportunity is surfaced without a point bonus',()=>{
  const hold=kicker('Incumbent',8.8,[2,1,1,1]),add=kicker('Alternative',8.9,[1,3,4,3]);
  const result=planWaivers([add],[hold],options([hold,add]))[0];
  assert.equal(result.lineupGain,.1);assert.equal(result.review.category,'close-call');
  assert.equal(result.review.kicking.addRate,10/3);assert.equal(result.review.kicking.holdRate,1);
  assert.equal(result.review.spendingSupported,false);assert.equal(result.faab.available,false);
  assert.match(result.review.counterarguments.join(' '),/effectively tied/);
  assert.match(actionPlanDetails(result),/not an instruction to acquire/);
  assert.match(actionPlanDetails(result),/If acquired/);
});
test('kicker stale, wrong-season and team-mismatch evidence cannot surface a near tie',()=>{
  const hold=kicker('Old',8.8,[2,1,1,1]);
  for(const override of [{season:2025},{team:'SEA'},{generatedAt:'2026-09-01T00:00:00Z'},{throughWeek:5},{datasetThroughWeek:3}]){
    const add=kicker('New',8.9,[1,3,4,3]);Object.assign(add.feature.opportunity.kicking,override);
    if(override.datasetThroughWeek)add.feature.dataThroughWeek=3;
    assert.equal(currentKicking(add,options([])),null);
    assert.equal(planWaivers([add],[hold],options([hold,add])).length,0);
  }
});
test('Dobbins shape: consistent low output and no workload catalyst is a watch, not a paid upgrade',()=>{
  const a=player('Lead','RB',23),b=player('Second','RB',13),qb=player('Starter QB','QB',18),spare=player('Spare QB','QB',16);
  const add=player('Volume only','RB',10.1,{status:'WAIVERS',feature:history([3.6,3.6,5.8,6.2])});
  const roster=[a,b,qb,spare],settings={...options([...roster,add]),slots:{0:1,2:2,20:1},rosterCapacity:4};
  const result=planWaivers([add],roster,settings)[0];
  assert.equal(result.drop.playerId,'Spare QB');assert.equal(result.lineupGain,0);
  assert.equal(result.review.category,'watch');assert.equal(result.review.spendingSupported,false);
  assert.equal(result.faab.available,false);assert.match(result.review.counterarguments.join(' '),/No observed workload-growth/);
});
test('a current sustained role and real starter benefit may support value-policy guidance',()=>{
  const old=player('Old','RB',10),add=player('Better','RB',15,{status:'WAIVERS',feature:history([14,13,16,14])});
  const result=planWaivers([add],[old],{...options([old,add]),slots:{2:1}})[0];
  assert.equal(result.review.category,'supported-upgrade');assert.equal(result.faab.available,true);
  assert.equal(result.faab.winProbability,null);assert.ok(result.review.missing.some(s=>s.includes('practice')));
});
test('TD-dependent production, stale role and questionable status withhold automatic spending',()=>{
  for(const extra of [{feature:history([15,16,13,15],{touchdownDependent:true})},{injuryStatus:'Q'},{feature:{...history([15,16,13,15]),generatedAt:'2026-09-01'}}]){
    const old=player('Old','RB',10),add=player('New','RB',15,{status:'WAIVERS',feature:history([15,16,13,15]),...extra});
    const r=planWaivers([add],[old],{...options([old,add]),slots:{2:1}})[0];
    assert.equal(r.faab.available,false);assert.notEqual(r.review.category,'supported-upgrade');
  }
});
test('bid guidance cannot be requested without a completed supporting review',()=>{
  assert.equal(bidGuidance({status:'WAIVERS',lineupGain:10,totalGain:20},options([]).market,now).available,false);
});
test('unsupported manual-review drafts start with a blank bid, never a paid league minimum',()=>{
  assert.equal(recommendationDraftBid({review:{spendingSupported:false},faab:{available:true,low:8}}),null);
  assert.equal(recommendationDraftBid({review:{spendingSupported:true},faab:{available:false}}),null);
  assert.equal(recommendationDraftBid({review:{spendingSupported:true},faab:{available:true,low:4}}),4);
});
test('kicking ingest rejects corrupt totals, duplicate games and impossible conversion counts',()=>{
  const row={week:4,eventId:'g4',team:'IND',attempts:3,made:2,patAttempts:1,patMade:1,longAttempts:1,longMade:1,unknownDistances:0};
  const k={...row,season:2026,games:1,throughWeek:4,generatedAt,weekly:[row]};
  assert.equal(validateKicking(k,{season:2026,throughWeek:4}),k);
  for(const bad of [{...k,attempts:5},{...k,weekly:[row,row],games:2},{...k,made:4},{...k,season:2025},{...k,weekly:[{...row,team:'SEA'}]}])assert.throws(()=>validateKicking(bad,{season:2026,throughWeek:4}),/kicking/i);
});
test('strong receiver pool cannot crowd out a material kicker near-tie',()=>{
  const hold=kicker('Old kicker',8.8,[2,1,1,1]),add=kicker('New kicker',8.9,[1,3,4,3]),wr=player('Old WR','WR',5);
  const pool=[add,...Array.from({length:60},(_,i)=>player(`WR${i}`,'WR',10+i/10))];
  const settings={...options([...pool,hold,wr]),slots:{17:1,4:1},rosterCapacity:2};
  const results=planWaivers(pool,[hold,wr],settings);
  assert.ok(results.some(p=>p.playerId==='New kicker'));assert.ok(results.length<=24);
  assert.equal(waiverReviewCoverage(pool,results).find(p=>p.position==='K').comparisons,1);
});
test('source conflicts are a critical availability review and UI output is escaped',()=>{
  const p=player('<script>','RB',15,{feature:history([14,14,14,14],{practice:{season:2026,week:5,fetchedAt:generatedAt,gameStatus:'Out'}}),lineupGain:5,totalGain:5,mode:'week',actionPlan:{startWeeks:[5]},roleEvidence:{signals:[]}});
  const r=reviewWaiverMove(p,[],options([]));assert.equal(r.critical,true);assert.equal(r.spendingSupported,false);
  r.whyNow='<script>alert(1)</script>';assert.ok(!reviewDetails(r).includes('<script>'));assert.match(reviewDetails(r),/Case against/);
});
test('frozen replay defaults to hold for reviews, separately evaluates projection-only baseline',()=>{
  const old=player('Old','RB',10),add=player('New','RB',15),settings={...options([old,add]),slots:{2:1}};
  const result=evaluateFrozenDecision({method:WAIVER_METHOD,inputs:{roster:[old],freeAgents:[add],settings,prospective:true}},{Old:10,New:20});
  assert.equal(result.recommendation,'Keep roster');assert.equal(result.proposedPoints,10);
  assert.equal(result.espnBaselinePoints,20);assert.equal(result.versusEspnBaseline,-10);
});
