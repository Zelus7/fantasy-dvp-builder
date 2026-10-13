import {auditFrozenDecision} from './model/decision-outcomes.js';
self.onmessage=async event=>{
  try{
    const {snapshot,actuals,observedUsage,feedback}=event.data,match=/^waiver-plan-v3:([a-f0-9]{12})$/.exec(snapshot.method);
    if(!match)throw new Error('Archived calculation version unavailable');
    const {evaluateFrozenDecision}=await import(`/model-history/${match[1]}/decision-outcomes.js`);
    const outcome=evaluateFrozenDecision(snapshot,actuals);
    const {planWaivers,weeklyRoster}=await import(`/model-history/${match[1]}/waiver-plan.js`);
    const {roster,freeAgents,settings}=snapshot.inputs,plans=planWaivers(freeAgents,roster,settings),hold=weeklyRoster(roster,settings,settings.currentWeek);
    const baseline=planWaivers(freeAgents,roster,{...settings,mode:'week',endWeek:settings.currentWeek,limit:48}).filter(p=>p.lineupGain>=.5).sort((a,b)=>b.lineupGain-a.lineupGain)[0];
    const audit=auditFrozenDecision(snapshot,actuals,outcome,{hold,plans,espnLineup:baseline?.comparison[0]?.starters||hold.starters},observedUsage,feedback);
    // Never silently reinterpret an old recommendation if its implementation
    // used a different selection rule. The original verdict remains untouched.
    if(['kept','proposed','espn'].some((key,i)=>{const score=[outcome.holdPoints,outcome.proposedPoints,outcome.espnBaselinePoints][i];return score!=null&&audit.lineups[key].points!==score;})){
      self.postMessage({...outcome,audit:null,auditWarning:'Detailed audit could not reproduce this older lineup selection. The original model result is preserved.'});return;
    }
    self.postMessage({...outcome,audit,forecastAccuracy:audit.forecastAccuracy});
  }catch{self.postMessage({status:'incompatible-model',explanation:'The archived calculation could not be loaded. This case is not counted as validated.'});}
};
