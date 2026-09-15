import {recommendWaiverMoves,discoverTradeTargets} from './model/decisions.js';
self.onmessage=event=>{
  try{
    const input=event.data;
    self.postMessage({recommendations:input.waiversReady?recommendWaiverMoves(input.freeAgents,input.roster,input.settings):[],trades:input.includeTrades&&input.adviceReady?discoverTradeTargets(input.teams,input.yourTeamId,input.settings):[]});
  }catch{self.postMessage({error:'The recommendation calculation failed. Refresh and try again.'})}
};
