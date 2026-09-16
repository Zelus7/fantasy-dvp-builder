self.onmessage=async event=>{
  try{
    const {snapshot,actuals}=event.data,match=/^waiver-plan-v3:([a-f0-9]{12})$/.exec(snapshot.method);
    if(!match)throw new Error('Archived calculation version unavailable');
    const {evaluateFrozenDecision}=await import(`/model-history/${match[1]}/decision-outcomes.js`);
    self.postMessage(evaluateFrozenDecision(snapshot,actuals));
  }catch{self.postMessage({status:'incompatible-model',explanation:'The archived calculation could not be loaded. This case is not counted as validated.'});}
};
