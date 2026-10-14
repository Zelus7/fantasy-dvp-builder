import {weeklyRoster,healthyBaseline} from './waiver-plan.js';
import {eligibleForSlot} from './analysis.js';
import {tradeValueContext,tradeValueBalance,tradePackageValue} from './trade-value.js';

const id=p=>String(p.playerId),round=n=>Math.round(n*10)/10;
const active=p=>![21,22].includes(Number(p.lineupSlotId));
const injured=p=>['OUT','IR','INJURY_RESERVE','DOUBTFUL','SUSPENDED','SUSPENSION'].includes(String(p.injuryStatus).toUpperCase());
const transferable=p=>['QB','RB','WR','TE'].includes(p.position)&&healthyBaseline(p)!=null;
const arrival=p=>({...p,isStarter:false,lineupSlotId:20});
export const TRADE_METHOD='calendar-roster-fit-v2';

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
  const valueContext=settings._valueContext||tradeValueContext(settings.leagueTeams||[{roster:yourRoster},{roster:theirRoster}],settings);
  const valueScenarios=scenarios.map(scenario=>({scenario,...tradeValueBalance(give,receive,valueContext,weeks,settings,scenario)}));
  const value=valueScenarios.find(s=>s.scenario==='planning'),balanced=valueScenarios.every(s=>s.balanced);
  const noNewGaps=scenarioResults.every(r=>['your','their'].every(side=>r[side].weeks.every(w=>w.newGaps===0)));
  const robust=complete&&scenarioResults.every(r=>r.your.gain/weeks.length>=.5&&r.their.gain/weeks.length>=.5);
  const viable=robust&&noNewGaps&&balanced;
  const conditional=extraMoves.your.adds.length+extraMoves.their.adds.length>0;
  return {viable,recommendation:viable?(conditional?'conditional-mutual-fit':'mutual-fit'):'needs-review',give,receive,
    giveValue:value.sent.value,receiveValue:value.received.value,imbalance:value.imbalance==null?null:round(value.imbalance*100),method:TRADE_METHOD,
    valueReview:{...value,scenarios:valueScenarios,context:valueContext,marketVerified:false,acceptanceProbability:null,
      policy:'At most 25% replacement-surplus imbalance in every injury bookend; secondary package pieces count at 80% to discourage quantity-for-quality offers. These are conservative screening choices, not empirically calibrated market prices.'},
    window:{start:weeks[0],end:weeks.at(-1),count:weeks.length},yourImpact:planning.your,theirImpact:planning.their,
    scenarioResults,complete,conditional,extraMoves:Object.fromEntries(Object.entries(extraMoves).map(([k,{after,...v}])=>[k,v])),
    reason:!complete?'Schedule or estimate coverage is incomplete; no full-window value claim.':!noNewGaps?'The exchange creates additional starting-slot gaps in at least one return scenario.':!balanced?'The exchanged league-relative surplus is incomplete, uninformative, or more than 25% apart in a return bookend. Roster fit alone does not establish a fair offer.':!robust?'Both teams do not improve by at least 0.5 starter points per week across every return scenario.':conditional?'Both rosters improve if the named extra acquisitions succeed. This is not a secured trade path.':'Both teams improve across the displayed weeks and all return bookends, and the position-adjusted exchange clears the balance screen. The other manager’s willingness is still unknown.',
    warnings:[`Assumes the trade and named extra moves complete before Week ${weeks[0]} games. Verify your league's trade review period.`,
      'Future weeks use healthy-game baselines, not calibrated exact-week forecasts. Injury scenarios have no assigned probabilities.',
      'The 25% surplus-balance, 80% secondary-piece weight and 0.5-point weekly-gain rules are screening policies, not trade-market prices or acceptance odds.',
      'Replacement levels come from the loaded league rosters and free-agent pool, not external rankings. Role changes and market sentiment can invalidate the comparison.',
      ...(conditional?['A free-agent addition is conditional: budget, competing claims and availability can invalidate this path.']:[]),
      ...(extraMoves.your.openSlots||extraMoves.their.openSlots?['Open roster spots have no invented value. Select an explicit conditional addition to compare a follow-up move.']:[])]};
}

export function discoverTradePlans(teams,yourTeamId,settings={}){
  const own=teams.find(t=>String(t.id)===String(yourTeamId));if(!own)return [];
  const weeks=tradeWindow(settings);if(!weeks.length)return [];
  const _valueContext=tradeValueContext(teams,settings),_baselineCache=new Map();
  const valueCache=new Map();
  const packageValue=players=>{const key=players.map(id).join(',');if(!valueCache.has(key))valueCache.set(key,tradePackageValue(players,_valueContext,weeks,settings));return valueCache.get(key)};
  const singleValue=p=>packageValue([p]).value;
  const eligible=roster=>{
    const all=roster.filter(transferable).sort((a,b)=>singleValue(b)-singleValue(a)||id(a).localeCompare(id(b)));
    const priority=['QB','RB','WR','TE'].flatMap(position=>all.filter(p=>p.position===position).slice(0,2));
    return [...new Set([...priority,...all])].slice(0,12);
  };
  const bundles=players=>players.flatMap((p,i)=>[[p],...players.slice(i+1).map(q=>[p,q])]);
  const yours=bundles(eligible(own.roster).filter(p=>!(settings.protectedIds||[]).map(String).includes(id(p)))),results=[];
  const prepared=roster=>{
    const lineup=weeklyRoster(roster,settings,weeks[0]);
    return {lineup,players:new Map(roster.map(p=>[id(p),p]))};
  };
  const ownBase=prepared(own.roster),removals=new Map();
  // Cheap fit screening guides search only. Every displayed offer still gets
  // the full bilateral optimizer, capacity, lock and injury-bookend checks.
  const fit=(roster,base,outgoing,incoming)=>{
    const key=roster.map(id).join(',')+':'+outgoing.map(id).join(',');
    if(!removals.has(key))removals.set(key,weeklyRoster(roster.filter(p=>!outgoing.some(o=>id(o)===id(p))),settings,weeks[0]));
    const remaining=removals.get(key);
    const upside=incoming.reduce((sum,p)=>{
      const slots=remaining.starters.filter(s=>eligibleForSlot(p,s.slotId));
      const floor=slots.length?Math.min(...slots.map(s=>s.estimate)):0;
      return sum+Math.max(0,healthyBaseline(p)-floor);
    },0);
    return upside-(base.lineup.value-remaining.value);
  };
  for(const team of teams.filter(t=>t!==own)){
    const pairs=[],theirBase=prepared(team.roster),theirBundles=bundles(eligible(team.roster));
    for(const give of yours)for(const receive of theirBundles){
      const sent=packageValue(give),received=packageValue(receive),diff=Math.abs(sent.value-received.value)/Math.max(sent.value,received.value,1);
      if(sent.complete&&received.complete&&sent.value>0&&received.value>0&&diff<=.25)pairs.push({give,receive,diff,fit:Math.min(fit(own.roster,ownBase,give,receive),fit(team.roster,theirBase,receive,give))});
    }
    // Up to eight per shape, including positional diversity, not just the four
    // closest raw-point packages. No exhaustive-search or acceptance claim.
    const shortlist=['1:1','2:2','2:1','1:2'].flatMap(shape=>{
      const group=pairs.filter(p=>`${p.give.length}:${p.receive.length}`===shape).sort((a,b)=>b.fit-a.fit||a.diff-b.diff);
      const diverse=new Map();for(const pair of group){const signature=pair.give.map(p=>p.position).sort().join('+')+':'+pair.receive.map(p=>p.position).sort().join('+');if(!diverse.has(signature))diverse.set(signature,pair);}
      return [...new Set([...group.slice(0,4),...[...diverse.values()].slice(0,2),...group.toSorted((a,b)=>a.diff-b.diff).slice(0,2),...group])].slice(0,8);
    });
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
      const result=evaluateTradePlan(own.roster,team.roster,give.map(id),receive.map(id),{...settings,...extra,_baselineCache,_valueContext});
      if(result.viable)results.push({...result,otherTeam:{id:team.id,name:team.name},searchCoverage:{balancedPairs:pairs.length,detailedPairs:shortlist.length,maximumPerTeam:32,exhaustive:false}});
    }
  }
  return results.sort((a,b)=>Math.min(b.yourImpact.gain,b.theirImpact.gain)-Math.min(a.yourImpact.gain,a.theirImpact.gain)).slice(0,settings.limit??8);
}
