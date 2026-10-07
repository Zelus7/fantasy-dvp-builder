import {discoverTradePlans} from './model/trade-plan.js';
import {planWaivers,injuryHolds} from './model/waiver-plan.js';
import {emergingWaiverTargets} from './model/waiver-signals.js';
import {waiverReviewCoverage} from './model/waiver-review.js';
self.onmessage=event=>{
  try{
    const input=event.data;
    const recommendations=input.waiversReady?planWaivers(input.freeAgents,input.roster,input.settings):[];
    self.postMessage({recommendations,reviewCoverage:waiverReviewCoverage(input.freeAgents,recommendations),emerging:input.waiversReady?emergingWaiverTargets(input.freeAgents,input.roster,input.settings):[],holds:input.waiversReady?injuryHolds(input.roster,input.freeAgents,input.settings):[],trades:input.includeTrades&&input.adviceReady?discoverTradePlans(input.teams,input.yourTeamId,input.settings):[]});
  }catch{self.postMessage({error:'The recommendation calculation failed. Refresh and try again.'})}
};
