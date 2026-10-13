import test from 'node:test';
import assert from 'node:assert/strict';
import {planWaivers,bidGuidance} from '../src/waiver-plan.js';
import {auditFrozenDecision,lineupAudit} from '../src/decision-outcomes.js';
import {forecastAccuracy} from '../src/forecast.js';
import {playerIntelligence} from '../src/intelligence.js';
import {forecastAudit,decisionAuditPanel} from '../public/forecast-ui.js';
import {planDetails,dropContext} from '../public/waiver-ui.js';

const now=Date.parse('2026-10-13T16:00:00Z');
const make=(id,position,points,extra={})=>({playerId:id,name:id,position,team:'BUF',proTeam:'BUF',median:points,projectedPoints:points,seasonProjectedPoints:points*17,
  status:'WAIVERS',injuryStatus:'ACTIVE',isAvailable:true,eligibleForRecommendation:true,hasEstimate:true,lineupSlotId:20,
  eligibleSlotIds:[{QB:0,RB:2,WR:4,TE:6,K:17,'D/ST':16}[position]],game:{kickoff:'2026-10-18T17:00:00Z'},...extra});
const settings=players=>({season:2026,currentWeek:6,endWeek:8,mode:'week',now,slots:{2:1,20:2},rosterCapacity:3,
  schedules:Object.fromEntries(players.map(p=>[p.playerId,{weeks:[6,7,8].map(week=>({week,bye:false,missingSchedule:false}))}]))});

test('waiver gains distinguish empty roster slot from the best legal same-drop alternative',()=>{
  const hold=make('Hold','RB',0),marks=make('Marks','RB',10),miller=make('Miller','RB',8.4),owned=make('Unavailable','RB',30,{onTeamId:2});
  const rows=planWaivers([marks,miller,owned],[hold],settings([hold,marks,miller,owned]));
  const top=rows.find(p=>p.playerId==='Marks');
  assert.equal(top.lineupGain,10);assert.equal(top.alternativeGain,1.6);assert.equal(top.waiverAlternative.name,'Miller');
  assert.equal(rows.find(p=>p.playerId==='Miller').alternativeGain,-1.6);
  const alone=planWaivers([marks],[hold],settings([hold,marks]))[0];assert.equal(alone.alternativeGain,null);
  assert.match(planDetails(top),/Two different gains/);assert.match(planDetails(top),/not a claim that the other player is free/);
});

test('scarcity-based budget policy uses marginal gain but never fabricates winning odds',()=>{
  const market={type:'FAAB',budget:250,remaining:200,minimumBid:0,verifiedAt:new Date(now).toISOString()};
  const move={status:'WAIVERS',position:'RB',lineupGain:10,totalGain:15,alternativeGain:1.6,waiverAlternative:{name:'Miller'},review:{spendingSupported:true},marketContext:{competition:{available:true,plausibleStarterRivals:3}}};
  const bid=bidGuidance(move,market,now);
  assert.equal(bid.high,5);assert.equal(bid.marginalGain,1.6);assert.equal(bid.winProbability,null);
  assert.match(bid.explanation,/3 rival team/);assert.match(bid.explanation,/not the full 10.0/);
  assert.equal(bidGuidance({...move,alternativeGain:null},market,now).high,20);
  assert.equal(bidGuidance({...move,review:{spendingSupported:false}},market,now).available,false);
});

test('zero-point and unknown-estimate insurance never enters the claim review queue',()=>{
  const qb=make('StarterQB','QB',20),spare=make('SpareQB','QB',18),te=make('StartingTE','TE',10);
  const zero=make('ZeroTE','TE',0),unknown=make('UnknownTE','TE',null,{hasEstimate:false,seasonProjectedPoints:50});
  const roster=[qb,spare,te],opts={...settings([...roster,zero,unknown]),slots:{0:1,6:1,20:1},rosterCapacity:3};
  assert.equal(planWaivers([zero,unknown],roster,opts).length,0);
});

test('backup QB drop discloses replacement scenario without deleting future cost',()=>{
  const qb=make('Maye','QB',20,{isStarter:true,lineupSlotId:0,cantCut:true}),spare=make('Nix','QB',18),rb=make('RB','RB',1,{cantCut:true}),add=make('Marks','RB',10),replacement=make('StreamQB','QB',16);
  const roster=[qb,spare,rb],opts={...settings([...roster,add,replacement]),slots:{0:1,2:1,20:1},rosterCapacity:3};
  opts.schedules.Maye.weeks.find(w=>w.week===7).bye=true;
  const r=planWaivers([add,replacement],roster,opts).find(p=>p.playerId==='Marks');
  assert.equal(r.drop.name,'Nix');assert.equal(r.dropCost,18);
  assert.equal(r.dropReplacement.weeks[0].replacement.name,'StreamQB');assert.equal(r.dropReplacement.weeks[0].residualLoss,2);
  assert.ok(r.futureCoverageWeeks.includes(7));assert.match(dropContext(r),/NOT guaranteed/);
  assert.match(dropContext({...r,dropCostComplete:false}),/—/);
});

test('matched forecast coverage reconciles exclusions and exposes signed player errors',()=>{
  const p=make('Player','WR',10,{forecast:{usable:true,targetWeek:6,horizons:{'1':{points:12,low:4,high:20}}}});
  const rows=[p,p,{...p,playerId:'no-model',forecast:null},{...p,playerId:'missing'},{...p,playerId:'late',game:{kickoff:'2026-10-10'}},{...p,playerId:'no-espn',projectedPoints:null}];
  const audit=forecastAccuracy(rows,{Player:8,'no-espn':7,late:20},6,now);
  assert.deepEqual(audit.coverage,{uniquePlayers:5,noUsableForecast:1,notPreKickoff:1,missingActual:1,missingEspn:1,matched:1});
  assert.equal(audit.rows[0].error,4);assert.equal(audit.rows[0].espnError,2);assert.equal(audit.missing.length,2);
  assert.equal(forecastAccuracy([p],{Player:0},6,now).rows[0].actual,0);
  audit.rows[0].name='<script>bad</script>';assert.doesNotMatch(forecastAudit(audit),/<script>/);
  assert.match(forecastAudit(audit),/each player counted once/);
});

test('retrospective separates start/sit, legal waiver gain, acquisition cost and usage retention',()=>{
  const current=make('Current','RB',8,{isStarter:true}),bench=make('Bench','RB',10),add=make('Add','RB',12);
  const snapshot={inputs:{roster:[current,bench],freeAgents:[add],settings:settings([current,bench,add]),prospective:true}};
  const hold={starters:[{...bench,estimate:10,slotId:2}]},plan={...add,drop:current,comparison:[{week:6,starters:[{...add,estimate:12,slotId:2}]}],roleEvidence:{signals:[{kind:'usage-rise',metric:'carries',value:20,week:5}]}};
  const outcome={plannedAddId:'Add',plannedDropId:'Current'},actuals={Current:5,Bench:15,Add:18};
  const source={hold,plans:[plan],espnLineup:hold.starters},usage={Add:[{week:6,team:'BUF',carries:16}]};
  const audit=auditFrozenDecision(snapshot,actuals,outcome,source,usage,{status:'acquired',playerId:'Add',cost:3});
  assert.equal(audit.metrics.startSitGain,10);assert.equal(audit.metrics.waiverNetGain,3);assert.equal(audit.metrics.faabPointsPerDollar,1);
  assert.equal(audit.metrics.rolePersistence[0].retainedRatio,.8);
  assert.equal(auditFrozenDecision(snapshot,actuals,outcome,source,usage,{status:'acquired',playerId:'Other',cost:3}).metrics.faabPointsPerDollar,null);
  assert.equal(auditFrozenDecision(snapshot,actuals,outcome,source,usage,{status:'acquired',playerId:'Add',cost:0}).metrics.faabPointsPerDollar,null);
  assert.equal(auditFrozenDecision(snapshot,actuals,outcome,source,{}).metrics.rolePersistence[0].after,null);
  assert.equal(lineupAudit([add],{}).points,null);assert.equal(lineupAudit([add],{Add:0}).points,0);
  assert.match(decisionAuditPanel(audit),/Separate decision results/);
});

test('old OUT roster label is not represented as a new official week designation',()=>{
  const p=make('Injured','WR',0,{injuryStatus:'OUT',feature:{season:2026,opportunity:{practice:{season:2026,week:5,fetchedAt:new Date(now).toISOString(),gameStatus:'Out'}}}});
  const result=playerIntelligence(p,{season:2026,week:6,now});
  assert.equal(result.availability.currentDesignation,false);assert.match(result.warnings.join(' '),/earlier OUT label does not establish another missed week/);
  assert.equal(p.injuryStatus,'OUT','a caution never silently clears the player');
  p.feature.opportunity.practice.week=6;
  assert.equal(playerIntelligence(p,{season:2026,week:6,now}).availability.currentDesignation,true);
});
