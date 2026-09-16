import {discoverTradeTargets} from './model/decisions.js';
import {planWaivers,injuryHolds} from './model/waiver-plan.js';
self.onmessage=event=>{
  try{
    const input=event.data;
    self.postMessage({recommendations:input.waiversReady?planWaivers(input.freeAgents,input.roster,input.settings):[],holds:input.waiversReady?injuryHolds(input.roster,input.freeAgents,input.settings):[],trades:input.includeTrades&&input.adviceReady?discoverTradeTargets(input.teams,input.yourTeamId,input.settings):[]});
  }catch{self.postMessage({error:'The recommendation calculation failed. Refresh and try again.'})}
};
