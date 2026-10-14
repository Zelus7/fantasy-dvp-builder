import test from 'node:test';
import assert from 'node:assert/strict';
import {planWaivers,injuryContingencies} from '../src/waiver-plan.js';
import {actionPlanDetails} from '../public/planning-ui.js';
const now=Date.parse('2026-10-14T12:00:00Z');
const p=(name,points,extra={})=>({playerId:name,name,position:'RB',proTeam:'TEN',median:points,projectedPoints:points,seasonProjectedPoints:points*17,hasEstimate:true,isAvailable:true,eligibleForRecommendation:true,injuryStatus:'ACTIVE',status:'ONTEAM',lineupSlotId:20,eligibleSlotIds:[2,23],game:{kickoff:'2026-10-18T17:00:00Z'},...extra});
const risk=p('Questionable starter',9.5,{injuryStatus:'QUESTIONABLE',isStarter:true,lineupSlotId:2});
const add=p('Insurance RB',7.5,{status:'FREEAGENT'});
const options={now,currentWeek:6,endWeek:6,mode:'week',slots:{2:1,20:2},rosterCapacity:3};
test('named starter-out counterfactual finds insurance without adding it to the normal forecast',()=>{
  const result=planWaivers([add],[risk],options)[0];
  assert.equal(result.lineupGain,0);assert.equal(result.waiverValue,0);assert.equal(result.totalGain,0);
  assert.equal(result.contingencies[0].name,risk.name);assert.equal(result.contingencies[0].gain,7.5);
  assert.equal(result.contingencies[0].holesAvoided,1);assert.equal(result.contingencies[0].probability,null);
  assert.equal(result.review.category,'injury-contingency');assert.equal(result.faab.available,false);
  assert.match(actionPlanDetails(result),/not extra expected points/);
});
test('insurance is measured against the existing backup, not the injured player or an invented empty slot',()=>{
  const backup=p('Existing backup',6);
  assert.equal(planWaivers([add],[risk,backup],options)[0].contingencies[0].gain,1.5);
  assert.equal(planWaivers([add],[risk,p('Better backup',8)],options).length,0);
  assert.equal(planWaivers([add],[{...risk,injuryStatus:'ACTIVE'}],options).length,0);
});
test('legal FLEX competition matters, and questionable/locked/missing-estimate cover is not recommended',()=>{
  const receiver=p('Receiver backup',8,{position:'WR',eligibleSlotIds:[4,23]});
  const flexRisk={...risk,lineupSlotId:23};
  assert.equal(planWaivers([add],[flexRisk,receiver],{...options,slots:{23:1,20:2}}).length,0);
  for(const override of [{injuryStatus:'Q'},{game:{kickoff:'2026-10-13T17:00:00Z'}},{game:null},{hasEstimate:false},{median:null},{isAvailable:false}]){
    assert.equal(planWaivers([{...add,...override}],[risk],options).length,0);
  }
  assert.equal(injuryContingencies(add,risk,[risk],options).length,0);
});
test('earlier insurance kickoff is explicit; protected and costly drops are not silently sacrificed',()=>{
  const late={...risk,game:{kickoff:'2026-10-19T00:20:00Z'}};
  const result=planWaivers([add],[late],options)[0];
  assert.equal(result.contingencies[0].earlierKickoff,true);assert.equal(result.contingencies[0].decisionBy,add.game.kickoff.replace('Z','.000Z'));
  assert.match(result.review.whyNow,/cannot wait/);
  const bench=p('Protected backup',1);
  assert.equal(planWaivers([add],[risk,bench],{...options,rosterCapacity:2,protectedIds:[bench.playerId]}).length,0);
  const future=p('Future starter',15,{isAvailable:false,game:null});
  const settings={...options,rosterCapacity:2,endWeek:7,schedules:Object.fromEntries([risk,future,add].map(v=>[v.playerId,{weeks:[{week:7,bye:false,missingSchedule:false}]}]))};
  assert.equal(planWaivers([add],[risk,future],settings).length,0);
});
