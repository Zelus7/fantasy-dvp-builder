import test from 'node:test';
import assert from 'node:assert/strict';
import {healthyBaseline,returnScenario,weeklyRoster,planWaivers,injuryHolds,bidGuidance,opportunityEvidence} from '../src/waiver-plan.js';
import {acquisitionRules} from '../src/espn.js';
import {validateInjuryEvidence} from '../src/injury-evidence.js';
const now=Date.parse('2026-09-15T20:00:00Z');
const make=(playerId,value,extra={})=>({playerId,name:playerId,position:'WR',team:'BUF',proTeam:'BUF',median:value,projectedPoints:value,seasonProjectedPoints:value*17,injuryStatus:'ACTIVE',hasEstimate:true,isAvailable:true,eligibleForRecommendation:true,eligibleSlotIds:[4,23],lineupSlotId:20,game:{kickoff:'2026-09-20T17:00:00Z'},...extra});
const schedules=players=>Object.fromEntries(players.map(p=>[p.playerId,{weeks:Array.from({length:16},(_,i)=>({week:i+2,bye:false,missingSchedule:false}))}]));
const options=players=>({slots:{4:1,20:2},rosterCapacity:3,currentWeek:2,endWeek:17,now,schedules:schedules(players)});
const brown=make('Brown',20,{injuryStatus:'IR',median:0,projectedPoints:0,isAvailable:false,eligibleForRecommendation:false,injuryEvidence:{earliestWeek:6,earlyWeek:6,planningWeek:7,lateWeek:9,reviewBy:'2026-09-18T23:00:00Z'}});
test('IR has healthy future value, but return scenarios never start it early',()=>{
  assert.equal(healthyBaseline(brown),20);assert.equal(returnScenario(brown,5,2,'early',now).available,false);
  assert.equal(returnScenario(brown,6,2,'early',now).available,true);assert.equal(returnScenario(brown,6,2,'planning',now).available,false);
  assert.equal(returnScenario(brown,7,2,'planning',now).available,true);
});
test('unknown and stale injury dates remain uncertain, not a medical forecast',()=>{
  const unknown={...brown,injuryEvidence:null};assert.equal(returnScenario(unknown,10,2,'planning',now).available,false);
  assert.equal(returnScenario(unknown,10,2,'early',now).available,true);
  assert.equal(returnScenario(brown,7,2,'planning',Date.parse('2026-09-19')).sourceCurrent,false);
});
test('Brown hold considers future starting benefit and zero IR slots',()=>{
  const players=[brown,make('Pierce',9.5),make('Robinson',7.5)],free=make('Replacement',10,{status:'FREEAGENT'});
  const hold=injuryHolds(players,[free],options([...players,free]))[0];
  assert.equal(hold.ir.slots,0);assert.ok(hold.scenarios.every(s=>s.holdGain>0));assert.match(hold.recommendation,/Hold favored/);
});
test('Pierce Sunday night fallback deadline uses Robinson earlier kickoff',()=>{
  const pierce=make('Pierce',9.5,{injuryStatus:'QUESTIONABLE',status:'FREEAGENT',game:{kickoff:'2026-09-21T00:20:00Z'}}),bench=make('Robinson',7.5);
  const results=planWaivers([pierce],[bench],options([pierce,bench]));
  assert.equal(results[0].decisionBy,'2026-09-20T17:00:00Z');assert.equal(results[0].benchAlternative.name,'Robinson');
});
test('a receiver fallback must fill its actual starting slot, not an unused ESPN superflex eligibility',()=>{
  const starter=make('Starting QB',25,{position:'QB',eligibleSlotIds:[0,7,20,21],isStarter:true,lineupSlotId:0}),qb=make('Bench QB',18,{position:'QB',eligibleSlotIds:[0,7,20,21]}),wr=make('Bench WR',7,{eligibleSlotIds:[4,7,23,20,21]}),add=make('Target WR',12,{eligibleSlotIds:[4,7,23,20,21],status:'WAIVERS'}),players=[starter,qb,wr,add];
  const result=planWaivers([add],[starter,qb,wr],{...options(players),slots:{0:1,4:1,20:2},rosterCapacity:4,endWeek:2})[0];
  assert.equal(result.benchAlternative.name,'Bench WR');
});
test('weekly upgrade compares whole legal starting lineup, without bench multiplier',()=>{
  const starter=make('Starter',10,{isStarter:true,lineupSlotId:4}),bench=make('Bench',9),add=make('Add',14,{status:'FREEAGENT'});
  const result=planWaivers([add],[starter,bench],options([starter,bench,add]))[0];
  assert.equal(result.lineupGain,4);assert.equal(result.waiverValue,4);assert.equal(result.drop,null);
});
test('routine defense and kicker streams never sacrifice skill-position depth',()=>{
  for(const [position,slot] of [['D/ST',16],['K',17]]){
    const starter=make('Old streamer',4,{position,eligibleSlotIds:[slot],isStarter:true,lineupSlotId:slot}),stash=make('Valuable stash',8),add=make('New streamer',7,{position,eligibleSlotIds:[slot],status:'FREEAGENT'}),players=[starter,stash,add];
    const opts={...options(players),slots:{[slot]:1,20:1},rosterCapacity:2};
    const result=planWaivers([add],[starter,stash],opts)[0];assert.equal(result.drop.playerId,starter.playerId);assert.equal(result.lineupGain,3);
    assert.equal(planWaivers([add],[{...starter,cantCut:true},stash],opts).length,0);
    assert.equal(planWaivers([add],[starter,stash],{...opts,rosterCapacity:3})[0].drop,null);
  }
});
test('protected, owned, unavailable and locked candidates cannot become weekly claims',()=>{
  const roster=[make('Old',8)],add=make('New',12,{status:'FREEAGENT'}),opts={...options([...roster,add]),rosterCapacity:1,protectedIds:['Old']};
  assert.equal(planWaivers([add],roster,opts).length,0);
  for(const extra of [{onTeamId:1},{status:'ONTEAM'},{isAvailable:false},{game:{kickoff:'2026-09-10T00:00:00Z'}}])assert.equal(planWaivers([{...add,...extra}],roster,options([...roster,add])).length,0);
});
test('missing future schedule is not interpreted as a bye or a valid season forecast',()=>{
  const p=make('Old',8),add=make('New',12,{status:'FREEAGENT'}),opts={...options([p,add]),schedules:{},mode:'ros'};
  assert.ok(weeklyRoster([p],opts,3).missing);assert.equal(planWaivers([add],[p],opts).length,0);
});
test('bid policy respects verified minimum and budget without pretending market odds',()=>{
  const market={type:'FAAB',budget:250,remaining:3,minimumBid:1,verifiedAt:new Date().toISOString()};
  const bid=bidGuidance({lineupGain:7,totalGain:20,position:'WR'},market);
  assert.equal(bid.max,3);assert.ok(bid.low>=1);assert.match(bid.explanation,/not your approved spending limit/);assert.equal(bid.winProbability,null);
  assert.equal(bidGuidance({}, {...market,remaining:0}).available,false);
  assert.equal(bidGuidance({}, {...market,verifiedAt:'bad'}).available,false);
  assert.equal(bidGuidance({status:'FREEAGENT'},market).available,false);
  assert.match(bidGuidance({status:'FREEAGENT'},market).explanation,/free agent, not on waivers/);
});
test('acquisition rules retain unknown as null and use true budget spent',()=>{
  assert.equal(acquisitionRules().remaining,null);
  assert.equal(acquisitionRules({acquisitionBudget:250,isUsingAcquisitionBudget:true,minimumBid:1},{transactionCounter:{acquisitionBudgetSpent:17}}).remaining,233);
});
test('workload evidence warns about small samples and touchdown reliance',()=>{
  const e=opportunityEvidence({feature:{opportunity:{games:1,targetShare:.2,carryShare:0,touchdownDependent:true}}});
  assert.equal(e.quality,'small-sample');assert.ok(e.warnings.some(w=>w.includes('touchdowns')));
});
test('return evidence requires ordered weeks and an HTTPS reference',()=>{
  const valid={playerId:'1',earliestWeek:6,earlyWeek:6,planningWeek:7,lateWeek:9,sourceUrl:'https://example.org/report'};
  assert.equal(validateInjuryEvidence(valid,2026).planningWeek,7);
  assert.throws(()=>validateInjuryEvidence({...valid,lateWeek:5},2026));
  assert.throws(()=>validateInjuryEvidence({...valid,sourceUrl:'javascript:bad'},2026));
});
test('a weekly stream with long-term cost is labeled a tradeoff, not hidden',()=>{
  const star=make('Star',2,{seasonProjectedPoints:340,isStarter:true,lineupSlotId:4}),add=make('Stream',12,{status:'FREEAGENT'});
  const results=planWaivers([add],[star],{...options([star,add]),rosterCapacity:1});
  assert.equal(results[0].tradeoff,true);assert.ok(results[0].totalGain<0);
});
test('dropping a reserve does not create an active slot and activating IR needs space',()=>{
  const active=make('Active',5),reserve=make('Reserve',20,{lineupSlotId:21}),add=make('Add',12,{status:'FREEAGENT'}),opts={...options([active,reserve,add]),rosterCapacity:1};
  assert.ok(planWaivers([add],[active,reserve],opts).every(p=>p.drop?.playerId!=='Reserve'));
  assert.ok(weeklyRoster([active,reserve],opts,3).missing>0);
});
test('a legal weekly stream is not silently removed for a later bye gap',()=>{
  const old=make('Old defense',5,{position:'D/ST',eligibleSlotIds:[16]}),add=make('New defense',9,{position:'D/ST',eligibleSlotIds:[16],status:'FREEAGENT'});
  const opts={...options([old,add]),rosterCapacity:1,slots:{16:1}};
  opts.schedules[old.playerId].weeks.find(w=>w.week===7).bye=true;
  opts.schedules[add.playerId].weeks.find(w=>w.week===8).bye=true;
  const result=planWaivers([add],[old],opts)[0];
  assert.equal(result.lineupGain,4);assert.ok(result.futureCoverageWeeks.includes(8));assert.ok(result.warnings.some(w=>w.includes('extra future')));
});
test('unknown injury timing uses conservative bookends rather than assuming zero future stash value',()=>{
  const unknown={...brown,injuryEvidence:null},bench=make('Bench',5),add=make('Add',15,{status:'FREEAGENT'});
  const result=planWaivers([add],[unknown,bench],{...options([unknown,bench,add]),mode:'ros'})[0];
  assert.equal(result.totalGain,Math.min(...result.scenarioTotals.map(s=>s.gain)));
  assert.equal(result.comparisonScenario,'conservative return bookend');
});
