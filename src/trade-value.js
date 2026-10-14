import {optimizeLineup} from './analysis.js';
import {healthyBaseline,returnScenario} from './waiver-plan.js';

const id=p=>String(p.playerId),round=n=>Math.round(n*10)/10;
const positions=['QB','RB','WR','TE'];
export const TRADE_VALUE_POLICY={imbalanceLimit:.25,secondaryWeight:.8};

// League-relative replacement value, NOT a sourced trade-market price. FLEX and
// superflex demand are allocated by the same legal lineup optimizer as the app.
export function tradeValueContext(teams,settings={}){
  const count=teams.length,unique=new Map([...teams.flatMap(t=>t.roster),...(settings.freeAgents||[])].map(p=>[id(p),p]));
  const players=[...unique.values()].filter(p=>positions.includes(p.position)&&healthyBaseline(p)!=null)
    .map(p=>({...p,decisionScore:healthyBaseline(p),isAvailable:true,hasEstimate:true,isStarter:false,game:null}));
  const slots=Object.fromEntries(Object.entries(settings.slots||{}).map(([slot,n])=>[slot,Number(n)*count]));
  const lineup=optimizeLineup(players,slots,[],0),starters=new Set(lineup.starters.map(id)),replacement={};
  for(const position of positions){
    const selected=lineup.starters.filter(p=>p.position===position),bench=players.filter(p=>p.position===position&&!starters.has(id(p)));
    const value=bench.length?Math.max(...bench.map(p=>p.decisionScore)):selected.length?Math.min(...selected.map(p=>p.decisionScore)):null;
    replacement[position]={value,starters:selected.length,observed:players.filter(p=>p.position===position).length,limited:!bench.length};
  }
  return {teamCount:count,players:players.length,replacement,method:'league-replacement-surplus-v1',
    explanation:'Healthy-game value above the best non-starting player at each position after filling every league starting slot, including FLEX. A thin pool uses the lowest selected starter as a conservative boundary. This is a league-relative screening index, not a market price or acceptance probability.'};
}

export function tradePackageValue(players,context,weeks,settings,scenario='planning'){
  const details=players.map(p=>{
    const replacement=context.replacement[p.position]?.value,baseline=healthyBaseline(p);
    let complete=replacement!=null&&baseline!=null,availableWeeks=0;
    for(const week of weeks){
      const game=settings.schedules?.[id(p)]?.weeks?.find(w=>w.week===week);
      const known=week===settings.currentWeek?!!p.game||p.isAvailable===false:!!game&&!game.missingSchedule;
      if(!known)complete=false;
      if(known&&!game?.bye&&returnScenario(p,week,settings.currentWeek,scenario,settings.now).available)availableWeeks++;
    }
    const surplus=complete?Math.max(0,baseline-replacement)*availableWeeks:null;
    return {playerId:id(p),name:p.name,position:p.position,baseline:round(baseline),replacement:replacement==null?null:round(replacement),availableWeeks,surplus:surplus==null?null:round(surplus),complete};
  });
  const ordered=details.map(p=>p.surplus??0).sort((a,b)=>b-a);
  return {complete:details.every(p=>p.complete),value:round(ordered.reduce((sum,v,i)=>sum+v*(i?TRADE_VALUE_POLICY.secondaryWeight:1),0)),players:details};
}

export function tradeValueBalance(give,receive,context,weeks,settings,scenario='planning'){
  const sent=tradePackageValue(give,context,weeks,settings,scenario),received=tradePackageValue(receive,context,weeks,settings,scenario);
  const informative=sent.value>0&&received.value>0;
  const imbalance=informative?Math.abs(sent.value-received.value)/Math.max(sent.value,received.value):null;
  return {sent,received,informative,complete:sent.complete&&received.complete,imbalance,
    balanced:sent.complete&&received.complete&&informative&&imbalance<=TRADE_VALUE_POLICY.imbalanceLimit};
}
