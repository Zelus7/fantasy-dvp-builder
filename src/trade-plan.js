import {weeklyRoster,healthyBaseline} from './waiver-plan.js';

const id=p=>String(p.playerId),round=n=>Math.round(n*10)/10;
const active=p=>![21,22].includes(Number(p.lineupSlotId));
const injured=p=>['OUT','IR','INJURY_RESERVE','DOUBTFUL','SUSPENDED','SUSPENSION'].includes(String(p.injuryStatus).toUpperCase());
const transferable=p=>['QB','RB','WR','TE'].includes(p.position)&&healthyBaseline(p)!=null;
const arrival=p=>({...p,isStarter:false,lineupSlotId:20});
export const TRADE_METHOD='calendar-roster-fit-v1';

export function tradeWindow(settings={}){
  const current=Number(settings.currentWeek),start=Number(settings.effectiveWeek??current);
  const end=Math.min(18,Number(settings.endWeek??17));
  if(!Number.isInteger(current)||!Number.isInteger(start)||start<current||start>end)return [];
  const last=settings.mode==='week'?start:settings.mode==='ros'?end:Math.min(end,start+3);
  return Array.from({length:last-start+1},(_,i)=>start+i);
}

function describe(before,after){
  const complete=before.every((w,i)=>!w.missing&&!after[i].missing);
  return {complete,before:complete?round(before.reduce((s,w)=>s+w.value,0)):null,
    after:complete?round(after.reduce((s,w)=>s+w.value,0)):null,
    gain:complete?round(after.reduce((s,w,i)=>s+w.value-before[i].value,0)):null,
    weeks:before.map((b,i)=>({week:b.week,before:b,after:after[i],gain:!b.missing&&!after[i].missing?round(after[i].value-b.value):null,
      newGaps:Math.max(0,after[i].unfilled-b.unfilled)}))};
}

// Evaluate explicit roster paths, not an assumed future waiver win. No ESPN writes.
export function evaluateTradePlan(yourRoster,theirRoster,giveIds,receiveIds,settings={}){
  const rejected=reason=>({viable:false,recommendation:'needs-review',reason,give:[],receive:[],method:TRADE_METHOD});
  const validIds=(ids,max)=>Array.isArray(ids)&&ids.length<=max&&new Set(ids.map(String)).size===ids.length;
  if(!validIds(giveIds,2)||!validIds(receiveIds,2)||!giveIds.length||!receiveIds.length)return rejected('Choose one or two distinct players on each side.');
  const give=yourRoster.filter(p=>giveIds.map(String).includes(id(p))),receive=theirRoster.filter(p=>receiveIds.map(String).includes(id(p)));
  const reject=reason=>({...rejected(reason),give,receive});
  if(give.length!==giveIds.length||receive.length!==receiveIds.length||give.some(p=>receive.some(r=>id(r)===id(p))))return reject('Every traded player must belong to the selected side.');
  if(give.some(p=>(settings.protectedIds||[]).map(String).includes(id(p))))return reject('A protected player is included.');
  if([...give,...receive].some(p=>!transferable(p)))return reject('An exchanged player has no usable healthy-game baseline.');
  const weeks=tradeWindow(settings),now=settings.now??Date.now();
  if(!weeks.length)return reject('Choose an effective week inside the remaining season.');
  if(weeks[0]===settings.currentWeek&&[...give,...receive].some(p=>p.game?.kickoff&&Date.parse(p.game.kickoff)<=now))return reject('An exchanged player has already started. Select a later effective week; completed points cannot be traded.');
  const free=settings.freeAgents||[],owned=new Set([...yourRoster,...theirRoster].map(id)),extraMoves={};
  for(const [side,roster,outgoing,incoming] of [['your',yourRoster,give,receive],['their',theirRoster,receive,give]]){
    const drops=settings[`${side}DropIds`]||[],adds=settings[`${side}AddIds`]||[];
    if(!validIds(drops,2)||!validIds(adds,1))return reject('Choose at most two extra drops and one conditional free-agent addition per team.');
    const dropPlayers=roster.filter(p=>drops.map(String).includes(id(p))),addPlayers=free.filter(p=>adds.map(String).includes(id(p)));
    if(dropPlayers.length!==drops.length||addPlayers.length!==adds.length)return reject('Extra roster moves must use players on that team or in the verified free-agent pool.');
    if(dropPlayers.some(p=>outgoing.some(g=>id(g)===id(p))||p.cantCut||side==='your'&&(settings.protectedIds||[]).map(String).includes(id(p))))return reject('An extra drop is protected, cannot be cut, or is already in the trade.');
    if(addPlayers.some(p=>owned.has(id(p))||Number(p.onTeamId)>0||!['WAIVERS','FREEAGENT'].includes(p.status)||injured(p)))return reject('A conditional addition must be available and not ruled out.');
    if(weeks[0]===settings.currentWeek&&[...dropPlayers,...addPlayers].some(p=>p.game?.kickoff&&Date.parse(p.game.kickoff)<=now))return reject('An extra move involves a locked player. Choose a later effective week.');
    const after=[...roster.filter(p=>![...outgoing,...dropPlayers].some(g=>id(g)===id(p))),...incoming.map(arrival),...addPlayers.map(arrival)];
    const capacity=Number(settings[`${side}RosterCapacity`]??settings.rosterCapacity);
    if(!Number.isInteger(capacity)||capacity<=0)return reject('A verified active-roster capacity is required.');
    if(after.filter(active).length>capacity)return {...reject(`${side==='your'?'Your':'Their'} team needs ${after.filter(active).length-capacity} additional drop(s) to fit the incoming players.`),requiredDrops:{[side]:after.filter(active).length-capacity}};
    extraMoves[side]={drops:dropPlayers,adds:addPlayers,openSlots:capacity-after.filter(active).length,after};
  }
  if(extraMoves.your.adds.some(p=>extraMoves.their.adds.some(q=>id(q)===id(p))))return reject('Both teams cannot acquire the same free agent.');
  const scenarios=[...yourRoster,...theirRoster].some(injured)?['early','planning','late']:['planning'];
  const scenarioResults=scenarios.map(scenario=>{
    const result={scenario};
    for(const [side,roster] of [['your',yourRoster],['their',theirRoster]]){
      const key=`${side}:${roster.map(id).join(',')}:${scenario}:${weeks.join(',')}`;
      const before=settings._baselineCache?.get(key)||weeks.map(w=>weeklyRoster(roster,settings,w,scenario));
      settings._baselineCache?.set(key,before);
      result[side]=describe(before,weeks.map(w=>weeklyRoster(extraMoves[side].after,settings,w,scenario)));
    }
    return result;
  });
  const planning=scenarioResults.find(r=>r.scenario==='planning'),complete=scenarioResults.every(r=>r.your.complete&&r.their.complete);
  const value=players=>players.reduce((s,p)=>s+healthyBaseline(p),0),giveValue=value(give),receiveValue=value(receive);
  const imbalance=Math.abs(giveValue-receiveValue)/Math.max(giveValue,receiveValue,1);
  const noNewGaps=scenarioResults.every(r=>['your','their'].every(side=>r[side].weeks.every(w=>w.newGaps===0)));
  const robust=complete&&scenarioResults.every(r=>r.your.gain/weeks.length>=.5&&r.their.gain/weeks.length>=.5);
  const viable=robust&&noNewGaps&&imbalance<=.2;
  const conditional=extraMoves.your.adds.length+extraMoves.their.adds.length>0;
  return {viable,recommendation:viable?(conditional?'conditional-mutual-fit':'mutual-fit'):'needs-review',give,receive,
    giveValue:round(giveValue),receiveValue:round(receiveValue),imbalance:round(imbalance*100),method:TRADE_METHOD,
    window:{start:weeks[0],end:weeks.at(-1),count:weeks.length},yourImpact:planning.your,theirImpact:planning.their,
    scenarioResults,complete,conditional,extraMoves:Object.fromEntries(Object.entries(extraMoves).map(([k,{after,...v}])=>[k,v])),
    reason:!complete?'Schedule or estimate coverage is incomplete; no full-window value claim.':!noNewGaps?'The exchange creates additional starting-slot gaps in at least one return scenario.':imbalance>.2?'More than 20% exchanged-baseline imbalance; not a balanced offer under this screen.':!robust?'Both teams do not improve by at least 0.5 starter points per week across every return scenario.':conditional?'Both rosters improve if the named extra acquisitions succeed. This is not a secured trade path.':'Both teams improve across the displayed weeks and all return bookends, with comparable exchanged baseline value.',
    warnings:[`Assumes the trade and named extra moves complete before Week ${weeks[0]} games. Verify your league's trade review period.`,
      'Future weeks use healthy-game baselines, not calibrated exact-week forecasts. Injury scenarios have no assigned probabilities.',
      'The 20% balance and 0.5-point weekly-gain rules are screening policies, not trade-market prices or acceptance odds.',
      ...(conditional?['A free-agent addition is conditional: budget, competing claims and availability can invalidate this path.']:[]),
      ...(extraMoves.your.openSlots||extraMoves.their.openSlots?['Open roster spots have no invented value. Select an explicit conditional addition to compare a follow-up move.']:[])]};
}

export function discoverTradePlans(teams,yourTeamId,settings={}){
  const own=teams.find(t=>String(t.id)===String(yourTeamId));if(!own)return [];
  const eligible=roster=>roster.filter(transferable).sort((a,b)=>healthyBaseline(b)-healthyBaseline(a)).slice(0,8);
  const bundles=players=>players.flatMap((p,i)=>[[p],...players.slice(i+1).map(q=>[p,q])]);
  const yours=bundles(eligible(own.roster).filter(p=>!(settings.protectedIds||[]).map(String).includes(id(p)))),results=[];
  const total=players=>players.reduce((s,p)=>s+healthyBaseline(p),0),_baselineCache=new Map();
  for(const team of teams.filter(t=>t!==own)){
    const pairs=[];
    for(const give of yours)for(const receive of bundles(eligible(team.roster))){
      const diff=Math.abs(total(give)-total(receive))/Math.max(total(give),total(receive),1);
      if(diff<=.2)pairs.push({give,receive,diff});
    }
    // Reserve room for each package shape, including 2-for-1; bounded search.
    const shortlist=['1:1','2:2','2:1','1:2'].flatMap(shape=>pairs.filter(p=>`${p.give.length}:${p.receive.length}`===shape).sort((a,b)=>a.diff-b.diff).slice(0,4));
    for(const {give,receive} of shortlist){
      const extra={};
      for(const [side,roster,outgoing,incoming] of [['your',own.roster,give,receive],['their',team.roster,receive,give]]){
        const excess=roster.filter(active).length-outgoing.filter(active).length+incoming.length-Number(settings.rosterCapacity);
        if(excess>0){
          const drops=roster.filter(p=>active(p)&&!p.cantCut&&!injured(p)&&!outgoing.some(g=>id(g)===id(p))&&!(side==='your'&&(settings.protectedIds||[]).map(String).includes(id(p))))
            .sort((a,b)=>(healthyBaseline(a)??Infinity)-(healthyBaseline(b)??Infinity)).slice(0,excess);
          extra[`${side}DropIds`]=drops.map(id);
        }
      }
      const result=evaluateTradePlan(own.roster,team.roster,give.map(id),receive.map(id),{...settings,...extra,_baselineCache});
      if(result.viable)results.push({...result,otherTeam:{id:team.id,name:team.name}});
    }
  }
  return results.sort((a,b)=>Math.min(b.yourImpact.gain,b.theirImpact.gain)-Math.min(a.yourImpact.gain,a.theirImpact.gain)).slice(0,settings.limit??8);
}
