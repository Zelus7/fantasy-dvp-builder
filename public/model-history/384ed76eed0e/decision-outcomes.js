import {planWaivers,WAIVER_METHOD,weeklyRoster} from './waiver-plan.js';

// Score the lineup selected with information available at decision time. Never
// optimize retrospectively using actual points (that would introduce hindsight).
export function evaluateFrozenDecision(snapshot,actuals){
  if(snapshot.method!==WAIVER_METHOD)return {status:'incompatible-model',reason:'The archived model version is not available for replay.'};
  const {roster,freeAgents,settings}=snapshot.inputs;
  const plans=planWaivers(freeAgents,roster,settings),hold=weeklyRoster(roster,settings,settings.currentWeek);
  const score=starters=>{
    const values=starters.map(p=>actuals[String(p.playerId)]);
    return values.every(v=>v!=null&&Number.isFinite(v))?Math.round(values.reduce((s,v)=>s+v,0)*100)/100:null;
  };
  const holdPoints=score(hold.starters),top=plans[0],lineup=top?.comparison.find(w=>w.week===settings.currentWeek)?.starters;
  const proposedPoints=lineup?score(lineup):holdPoints;
  // Simple ESPN-only baseline: maximize selected-week projection improvement,
  // with legal add/drop constraints, ignoring future injury-return advantages.
  const espnOnly=planWaivers(freeAgents,roster,{...settings,mode:'week',endWeek:settings.currentWeek})[0];
  const espnLineup=espnOnly?.comparison[0]?.starters||hold.starters,espnBaselinePoints=score(espnLineup);
  return {status:holdPoints==null||proposedPoints==null?'awaiting-complete-scores':'scored',holdPoints,proposedPoints,
    eligibleForValidation:snapshot.inputs.prospective===true,plannedAddId:top?.playerId||null,plannedDropId:top?.drop?.playerId||null,
    starterGain:holdPoints!=null&&proposedPoints!=null?Math.round((proposedPoints-holdPoints)*100)/100:null,
    espnBaselinePoints,versusEspnBaseline:espnBaselinePoints!=null&&proposedPoints!=null?Math.round((proposedPoints-espnBaselinePoints)*100)/100:null,
    recommendation:top?`Add ${top.name}; ${top.drop?`drop ${top.drop.name}`:'open slot'}`:'Keep roster',
    droppedPlayerOutscoredAdd:top?.drop&&actuals[top.drop.playerId]!=null&&actuals[top.playerId]!=null?actuals[top.drop.playerId]>actuals[top.playerId]:null,
    acquisitionVerified:false,explanation:'Counterfactual starter points for frozen lineups. This does not prove you won a claim or made a transaction; no acquisition cost is inferred.'};
}
