import {planWaivers,WAIVER_METHOD,weeklyRoster} from './waiver-plan.js';
import {forecastAccuracy} from './forecast.js';

const finite=n=>typeof n==='number'&&Number.isFinite(n),round=n=>Math.round(n*100)/100;
export function lineupAudit(starters,actuals){
  const rows=starters.map(p=>({playerId:String(p.playerId),name:p.name,slotId:p.slotId??p.lineupSlotId,estimate:p.estimate??p.median??null,
    actual:finite(actuals[String(p.playerId)])?actuals[String(p.playerId)]:null}));
  const missing=rows.filter(p=>p.actual==null);
  return {rows,missing:missing.map(p=>({playerId:p.playerId,name:p.name})),matched:rows.length-missing.length,expected:rows.length,
    points:missing.length?null:round(rows.reduce((n,p)=>n+p.actual,0))};
}

// Descriptive audit only. Callers supply lineups from the EXACT archived model;
// this function never reselects players using new logic or observed outcomes.
export function auditFrozenDecision(snapshot,actuals,outcome,{hold,plans,espnLineup},observedUsage={},feedback=null){
  const {roster,freeAgents,settings}=snapshot.inputs,players=[...roster,...freeAgents];
  const top=plans.find(p=>String(p.playerId)===String(outcome.plannedAddId)&&String(p.drop?.playerId??'')===String(outcome.plannedDropId??''));
  const proposed=top?.comparison.find(w=>w.week===settings.currentWeek)?.starters||hold.starters;
  const current=roster.filter(p=>p.isStarter===true),lineups={kept:lineupAudit(hold.starters,actuals),proposed:lineupAudit(proposed,actuals),espn:lineupAudit(espnLineup,actuals),asSet:current.length?lineupAudit(current,actuals):null};
  const difference=(a,b)=>a!=null&&b!=null?round(a-b):null;
  const waiverGain=difference(lineups.proposed.points,lineups.kept.points);
  const acquired=feedback?.status==='acquired'&&String(feedback.playerId)===String(outcome.plannedAddId);
  const rolePersistence=plans.flatMap(p=>(p.roleEvidence?.signals||[]).filter(s=>s.kind==='usage-rise'&&s.metric&&finite(s.value)).map(s=>{
    const row=(observedUsage[String(p.playerId)]||[]).find(w=>Number(w.week)===settings.currentWeek&&w.team===(p.team||p.proTeam));
    const value=finite(row?.[s.metric])?row[s.metric]:null;
    return {playerId:String(p.playerId),name:p.name,metric:s.metric,beforeWeek:s.week,before:s.value,afterWeek:settings.currentWeek,after:value,
      retainedRatio:value!=null&&s.value>0?round(value/s.value):null};
  }));
  const kickoffs=players.map(p=>Date.parse(p.game?.kickoff)).filter(Number.isFinite);
  return {lineups,forecastAccuracy:forecastAccuracy(players,actuals,settings.currentWeek,settings.now),
    timing:{frozenAt:finite(settings.now)?new Date(settings.now).toISOString():null,firstKickoff:kickoffs.length?new Date(Math.min(...kickoffs)).toISOString():null,prospective:snapshot.inputs.prospective===true},
    metrics:{startSitGain:lineups.asSet?difference(lineups.kept.points,lineups.asSet.points):null,waiverNetGain:waiverGain,
      faabPointsPerDollar:acquired&&finite(feedback.cost)&&feedback.cost>0&&waiverGain!=null?round(waiverGain/feedback.cost):null,
      acquisitionStatus:acquired?(feedback.cost===0?'User-reported free acquisition':feedback.cost==null?'User-reported acquisition; cost unknown':'User-reported acquisition and cost'):'No matching acquisition recorded',
      rolePersistence},
    explanation:'Start/sit gain compares the frozen suggested keep-roster lineup with the lineup set at that time. Waiver gain compares complete legal add/drop and keep lineups; it includes the drop, not raw add-minus-drop bench points. These are separate counterfactual decisions, not proof you executed them. FAAB efficiency requires a matching reported acquisition and known positive cost; it is one-week net starter points per dollar, not long-term value.'};
}

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
  const holdPoints=score(hold.starters),top=plans.find(p=>p.review?.spendingSupported),lineup=top?.comparison.find(w=>w.week===settings.currentWeek)?.starters;
  const proposedPoints=lineup?score(lineup):holdPoints;
  // Simple ESPN-only baseline: maximize selected-week projection improvement,
  // with legal add/drop constraints, ignoring future injury-return advantages.
  const espnOnly=planWaivers(freeAgents,roster,{...settings,mode:'week',endWeek:settings.currentWeek,limit:48}).filter(p=>p.lineupGain>=.5).sort((a,b)=>b.lineupGain-a.lineupGain)[0];
  const espnLineup=espnOnly?.comparison[0]?.starters||hold.starters,espnBaselinePoints=score(espnLineup);
  const outcome={status:holdPoints==null||proposedPoints==null||espnBaselinePoints==null?'awaiting-complete-scores':'scored',holdPoints,proposedPoints,
    eligibleForValidation:snapshot.inputs.prospective===true,plannedAddId:top?.playerId||null,plannedDropId:top?.drop?.playerId||null,
    forecastAccuracy:forecastAccuracy([...roster,...freeAgents],actuals,settings.currentWeek,settings.now),
    starterGain:holdPoints!=null&&proposedPoints!=null?Math.round((proposedPoints-holdPoints)*100)/100:null,
    espnBaselinePoints,versusEspnBaseline:espnBaselinePoints!=null&&proposedPoints!=null?Math.round((proposedPoints-espnBaselinePoints)*100)/100:null,
    recommendation:top?`Add ${top.name}; ${top.drop?`drop ${top.drop.name}`:'open slot'}`:'Keep roster',
    droppedPlayerOutscoredAdd:top?.drop&&actuals[top.drop.playerId]!=null&&actuals[top.playerId]!=null?actuals[top.drop.playerId]>actuals[top.playerId]:null,
    reviews:plans.map(p=>({playerId:p.playerId,category:p.review.category,spendingSupported:p.review.spendingSupported})),
    acquisitionVerified:false,explanation:'Counterfactual starter points for frozen lineups. Only evidence-supported upgrades count as model recommendations; watch/insurance/close-call reviews default to holding. This does not prove you won a claim or made a transaction; no acquisition cost is inferred.'};
  return {...outcome,audit:auditFrozenDecision(snapshot,actuals,outcome,{hold,plans,espnLineup})};
}
