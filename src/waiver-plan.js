import {optimizeLineup,eligibleForSlot} from './analysis.js';
import {playerIntelligence} from './intelligence.js';
import {MODEL_REVISION} from './model-revision.js';
import {learnedFallback} from './forecast.js';
import {waiverMarketContext,historicalBidContext} from './waiver-market.js';
import {STARTER_SLOT_IDS} from './constants.js';
import {waiverRoleSignals,waiverActionPlan} from './waiver-signals.js';
import {kickingEvidence,kickingComparison,reviewWaiverMove} from './waiver-review.js';

export const WAIVER_METHOD=`waiver-plan-v3:${MODEL_REVISION.slice(0,12)}`;
const id=p=>String(p.playerId), round=n=>Math.round(n*10)/10;
const finite=v=>v!=null&&Number.isFinite(Number(v));
const injured=p=>['OUT','IR','INJURY_RESERVE','DOUBTFUL','SUSPENDED','SUSPENSION'].includes(String(p.injuryStatus).toUpperCase());
const locked=(p,now)=>p.game?.kickoff&&Date.parse(p.game.kickoff)<=now;

// This is a healthy-game baseline, never a promise of availability or recovery.
export function healthyBaseline(p,week=null,currentWeek=null){
  if(p.team==='FA'||p.proTeam==='FA')return null;
  const history=finite(p.feature?.seasonPpg)?Number(p.feature.seasonPpg):null;
  if(injured(p)&&history!=null)return Math.max(0,history);
  // Historical qualification is not evidence of superiority over ESPN. Use a
  // learned fallback only when both ESPN projection sources are absent, and
  // only inside the tested four-week window; never extrapolate it through ROS.
  const learned=week>currentWeek&&week<currentWeek+4&&!finite(p.projectedPoints)&&!finite(p.seasonProjectedPoints)?learnedFallback(p,4):null;
  if(learned!=null)return Math.max(0,learned);
  const projection=finite(p.seasonProjectedPoints)&&Number(p.seasonProjectedPoints)>0?Number(p.seasonProjectedPoints)/17:
    !injured(p)&&finite(p.projectedPoints)?Number(p.projectedPoints):null;
  if(history==null&&projection==null)return null;
  const weight=Math.min(.65,Math.max(0,Number(p.feature?.currentGames||0))/10);
  return Math.max(0,projection==null?history:history==null?projection:projection*(1-weight)+history*weight);
}

export function returnScenario(p,week,currentWeek,scenario='planning',now=Date.now()){
  if(week===currentWeek)return {available:p.isAvailable!==false&&p.eligibleForRecommendation!==false,uncertain:false};
  if(!injured(p))return {available:true,uncertain:['Q','QUESTIONABLE'].includes(p.injuryStatus)};
  const evidence=p.injuryEvidence,valid=evidence&&Date.parse(evidence.reviewBy)>now;
  const minimum=Number(evidence?.earliestWeek)||currentWeek+1;
  // Unknown return: bookends only, not a fabricated expected return date.
  const returnWeek=valid?Number(evidence[`${scenario}Week`]??(scenario==='early'?minimum:19)):scenario==='early'?minimum:19;
  return {available:week>=Math.max(minimum,returnWeek),uncertain:true,returnWeek,sourceCurrent:!!valid};
}

export function weeklyRoster(roster,settings,week,scenario='planning'){
  const {slots={},schedules={},currentWeek=1,now=Date.now()}=settings;
  const activeCount=roster.filter(p=>![21,22].includes(Number(p.lineupSlotId))).length;
  let missing=0;
  const players=roster.map(p=>{
    const game=schedules[id(p)]?.weeks?.find(w=>w.week===week);
    const known=week===currentWeek?!!p.game||p.isAvailable===false:!!game&&!game.missingSchedule;
    if(!known)missing++;
    const reserved=[21,22].includes(Number(p.lineupSlotId));
    const activationBlocked=reserved&&(week===currentWeek||!(Number(settings.rosterCapacity)>activeCount));
    if(activationBlocked&&week!==currentWeek&&returnScenario(p,week,currentWeek,scenario,now).available)missing++;
    const available=!activationBlocked&&returnScenario(p,week,currentWeek,scenario,now).available&&known&&!game?.bye;
    const value=week===currentWeek?(finite(p.median)?Number(p.median):null):healthyBaseline(p,week,currentWeek);
    if(available&&value==null)missing++;
    return {...p,decisionScore:value??0,hasEstimate:value!=null,isAvailable:available,
      ...(week!==currentWeek?{game:null,isStarter:false}:{}),planValue:value??0};
  });
  const lineup=optimizeLineup(players,slots,week===currentWeek?players:[],week===currentWeek?now:0);
  return {week,value:round(lineup.starters.reduce((sum,p)=>sum+p.planValue,0)),missing,
    unfilled:lineup.unfilledSlots.length,starters:lineup.starters.map(p=>({playerId:id(p),name:p.name,slotId:p.assignedSlotId,estimate:round(p.planValue)}))};
}

export function opportunityEvidence(p,context={}){
  if(p.position==='K')return kickingEvidence(p,{...context,currentWeek:context.currentWeek??context.week});
  const o=p.feature?.opportunity;
  if(!o)return {quality:'missing',items:['Current-season workload evidence is unavailable.'],warnings:['Do not infer a role change from points alone.']};
  const pct=v=>`${Math.round(v*100)}%`,items=[],warnings=[];
  if(finite(o.targetShare))items.push(`${pct(o.targetShare)} of team targets across ${o.games} observed game(s).`);
  if(finite(o.carryShare))items.push(`${pct(o.carryShare)} of team rushing attempts.`);
  if(finite(o.snapShare))items.push(`${pct(o.snapShare)} offensive snap share in the latest available game (week ${o.snapWeek}).`);
  if(finite(o.recentTargets))items.push(`${o.recentTargets.toFixed(1)} targets/game over the last ${o.recentGames} observed game(s).`);
  if(finite(o.targetTrend))items.push(`${o.targetTrend>=0?'+':''}${o.targetTrend.toFixed(1)} targets/game versus earlier current-season games.`);
  if(finite(o.redZoneOpportunities))items.push(`${o.redZoneOpportunities} carries + targets inside the opponent's 20 across the observed games.`);
  if(o.depthPosition)items.push(`Latest listed depth-chart position: ${o.depthPosition}; depth charts are context, not guaranteed workload.`);
  if(o.games<3)warnings.push('Small current-season sample: no established role trend yet.');
  if(o.touchdownDependent)warnings.push('Recent production includes touchdowns on limited touches; repeatable volume is not established.');
  if(o.snapShare==null)warnings.push('Snap-share data is missing.');
  if(o.redZoneOpportunities==null)warnings.push('Scoring-area usage data is missing.');
  const intelligence=playerIntelligence(p,context);items.push(...intelligence.items);warnings.push(...intelligence.warnings);
  return {quality:o.games>=3?'observed':'small-sample',items,warnings,throughWeek:o.throughWeek,source:o.source};
}

export function bidGuidance(move,market={},now=Date.now()){
  if(move.status==='FREEAGENT')return {available:false,explanation:'ESPN currently lists this player as a free agent, not on waivers. Check the ESPN Add screen for availability and any acquisition cost; do not treat a FAAB policy range as a required bid.'};
  if(!move.review?.spendingSupported)return {available:false,policyCeiling:null,winProbability:null,explanation:'No automated paid-bid range: the review has not established an evidence-supported starting benefit. Hold, watch, or consider free/zero-bid insurance only after reviewing the drop and league minimum. This is not a prediction that a zero bid will win.'};
  const {budget,remaining,minimumBid,type,verifiedAt}=market;
  if(type!=='FAAB'||![budget,remaining,minimumBid].every(finite)||!Number.isFinite(Date.parse(verifiedAt))||now<Date.parse(verifiedAt)||now-Date.parse(verifiedAt)>300000)
    return {available:false,explanation:'No bid: a fresh FAAB budget, remaining balance and minimum bid are required. Check ESPN league settings.'};
  const min=Math.max(0,Math.ceil(minimumBid)),balance=Math.max(0,Math.floor(remaining));
  if(balance<min)return {available:false,explanation:'Remaining FAAB is below the league minimum bid.'};
  // An explicit spending policy, not fitted winning odds or a dollar value of points.
  // Scarcity is the gain over another legal same-drop option, not over an empty
  // slot. This is still a spending policy, not a learned auction-price model.
  const marginal=finite(move.alternativeGain)?Math.max(0,Number(move.alternativeGain)):move.lineupGain;
  const fraction=marginal>=5?.08:marginal>=2?.04:move.totalGain>5&&marginal>=.5?.02:.01;
  const cap=Math.min(balance,Math.max(min,Math.floor(budget*fraction)));
  const history=historicalBidContext(move.position,market,now);
  const low=Math.min(cap,Math.max(min,Math.floor(cap*.5))),high=cap;
  const demand=move.marketContext?.competition;
  return {available:true,low,high,max:cap,policyCeiling:cap,remaining:balance,minimumBid:min,sampleSize:history.sampleSize,marketMedian:history.median,winProbability:null,marginalGain:marginal,
    explanation:`Conservative value-policy range: ${Math.round(fraction*100)}% of the original $${budget} budget at the upper end, limited by $${balance} remaining. This is not your approved spending limit or a market-clearing bid. ${move.waiverAlternative?`The scarcity input is ${marginal.toFixed(1)} incremental starter points above ${move.waiverAlternative.name}, not the full ${move.lineupGain.toFixed(1)} gain over your roster. That alternative may also require a bid and is not guaranteed.`:'No same-drop alternative was found in the loaded pool; scarcity may be overstated.'} ${history.sampleSize?`Recent ${move.position} winning prices range from $${history.low} to $${history.high} (${history.sampleSize} claims); player quality differs.${history.high>cap?' This policy range could be too low for the market.':''}`:'Verified recent winning prices are unavailable; do not assume this range will win.'} ${demand?.available?`${demand.plausibleStarterRivals} rival team(s) could use this player in their modeled lineup before choosing a drop. `:''}Future coverage and drop costs are shown separately; review them before setting an exact bid. No reliable success percentage is available.`};
}

function replacementCoverage(drop,roster,candidates,future,lookahead,settings){
  if(!drop||drop.position!=='QB')return null;
  const without=roster.filter(p=>id(p)!==id(drop)),rows=[];
  for(const [i,week] of lookahead.entries()){
    const absent=weeklyRoster(without,settings,week);
    if(future[i].missing||absent.missing||future[i].value<=absent.value)continue;
    const choices=candidates.filter(p=>p.position==='QB').map(p=>({player:p,lineup:weeklyRoster([...without,{...p,lineupSlotId:20,isStarter:false}],settings,week)})).filter(x=>!x.lineup.missing)
      .sort((a,b)=>b.lineup.value-a.lineup.value||id(a.player).localeCompare(id(b.player)));
    const best=choices[0];
    rows.push({week,uncoveredLoss:round(future[i].value-absent.value),replacement:best?{playerId:id(best.player),name:best.player.name,status:best.player.status}:null,
      residualLoss:best?round(Math.max(0,future[i].value-best.lineup.value)):null});
  }
  return rows.length?{weeks:rows,explanation:'Backup-QB replacement stress test using players available today and healthy-game baselines. Future availability, price and a roster slot are NOT guaranteed. You would need another legal add/drop. These hypothetical replacements do not erase the no-further-transactions drop cost.'}:null;
}

function deadline(add,roster,now,drop,slots,targetSlot=null){
  const relevantSlots=targetSlot==null?Object.keys(slots).map(Number).filter(slot=>STARTER_SLOT_IDS.has(slot)&&Number(slots[slot])>0&&eligibleForSlot(add,slot)):[targetSlot];
  const backups=roster.filter(p=>!p.isStarter&&![21,22].includes(Number(p.lineupSlotId))&&!injured(p)&&p.eligibleForRecommendation!==false&&p.game?.kickoff&&Date.parse(p.game.kickoff)>now&&
    relevantSlots.some(slot=>eligibleForSlot(p,slot))).sort((a,b)=>(b.median??0)-(a.median??0));
  const backup=backups[0]||null,times=[add.game?.kickoff,backup?.game?.kickoff,drop?.game?.kickoff].filter(t=>t&&Date.parse(t)>now);
  return {benchAlternative:backup?{playerId:id(backup),name:backup.name,estimate:backup.median,kickoff:backup.game.kickoff}:null,
    decisionBy:times.sort((a,b)=>Date.parse(a)-Date.parse(b))[0]||null,
    claimDeadline:add.waiverProcessAt||null,
    explanation:'Decision-by is the earliest add, drop or fallback kickoff, not the waiver processing deadline. ESPN claim processing must finish before the player can be used.'};
}

export function planWaivers(freeAgents,roster,settings={}){
  const {mode='week',slots={},protectedIds=[],rosterCapacity=null,limit=24,now=Date.now(),currentWeek=1,endWeek=17}=settings;
  const protectedSet=new Set(protectedIds.map(String)),owned=new Set(roster.map(id));
  const capacity=Number(rosterCapacity),active=roster.filter(p=>![21,22].includes(Number(p.lineupSlotId))).length;
  // Injury drops require a separate explicit hold review, never an automatic churn suggestion.
  const drops=capacity>active?[null]:roster.filter(p=>![21,22].includes(Number(p.lineupSlotId))&&!protectedSet.has(id(p))&&!p.cantCut&&!injured(p)&&!locked(p,now));
  const candidates=freeAgents.filter(p=>!owned.has(id(p))&&['FREEAGENT','WAIVERS'].includes(p.status)&&Number(p.onTeamId||0)<=0&&!locked(p,now)&&!injured(p)&&healthyBaseline(p)!=null&&(mode!=='week'||p.isAvailable!==false&&p.eligibleForRecommendation!==false));
  const baseline=weeklyRoster(roster,settings,currentWeek),quick=[];
  const qb=roster.find(p=>p.position==='QB'),qbSlots=qb?Object.entries(slots).reduce((n,[slot,count])=>n+(STARTER_SLOT_IDS.has(Number(slot))&&eligibleForSlot(qb,Number(slot))?Number(count):0),0):0;
  const spareQuarterback=roster.filter(p=>p.position==='QB'&&!injured(p)&&![21,22].includes(Number(p.lineupSlotId))).length>qbSlots;
  const lookahead=Array.from({length:Math.max(0,(mode==='ros'?endWeek:Math.min(endWeek,currentWeek+3))-currentWeek)},(_,i)=>currentWeek+i+1);
  const future=lookahead.map(w=>weeklyRoster(roster,settings,w));
  // Optimistic screening only: the final comparison always includes the drop.
  // Include future add value so a bye-week replacement is not crowded out by
  // dozens of players with the same zero current-week gain.
  const futureAddValue=new Map(mode==='week'?[]:candidates.map(add=>[id(add),lookahead.reduce((sum,w,i)=>{
    const after=weeklyRoster([...roster,{...add,isStarter:false,lineupSlotId:20}],settings,w);
    return sum+(!future[i].missing&&!after.missing?Math.max(0,after.value-future[i].value):0);
  },0)]));
  const dropCosts=new Map(drops.filter(Boolean).map(drop=>{
    const remaining=roster.filter(p=>id(p)!==id(drop));
    let complete=true;
    const lost=lookahead.reduce((sum,w,i)=>{const after=weeklyRoster(remaining,settings,w);complete&&=!future[i].missing&&!after.missing;return sum+(!future[i].missing&&!after.missing?Math.max(0,future[i].value-after.value):0);},0);
    const slotsForPosition=Object.entries(slots).reduce((n,[slot,count])=>n+(STARTER_SLOT_IDS.has(Number(slot))&&eligibleForSlot(drop,Number(slot))?Number(count):0),0);
    const depth=roster.filter(p=>p.position===drop.position&&!injured(p)).length/Math.max(1,slotsForPosition);
    return [id(drop),{lost,depth,complete}];
  }));
  const last=mode==='bridge'?Math.min(endWeek,currentWeek+3):endWeek;
  // The drop-cost screen is near-term, but the displayed scenario can extend
  // through the season. A backup's later bye-week use still needs a replacement
  // stress test (for example a Week 11 QB bye during a Week 6 waiver review).
  const replacementWeeks=Array.from({length:Math.max(0,last-currentWeek)},(_,i)=>currentWeek+i+1);
  const replacementBaseline=spareQuarterback?replacementWeeks.map(w=>weeklyRoster(roster,settings,w)):[];
  const replacementByDrop=new Map(drops.filter(p=>p?.position==='QB'&&spareQuarterback).map(p=>[id(p),replacementCoverage(p,roster,candidates,replacementBaseline,replacementWeeks,settings)]));
  // Screen all legal pairs first; the most promising 48 moves receive the full
  // multiweek/scenario comparison. The UI discloses this bounded search.
  for(const add of candidates)for(const drop of drops){
    // Routine streaming must not consume a skill-position stash or leave two
    // defenses/kickers. Cross-position sacrifices require a deliberate review.
    if(['K','D/ST'].includes(add.position)&&drop&&drop.position!==add.position)continue;
    if(add.position==='QB'&&spareQuarterback&&drop&&drop.position!=='QB')continue;
    const afterRoster=[...roster.filter(p=>!drop||id(p)!==id(drop)),{...add,isStarter:false,lineupSlotId:20}];
    const after=weeklyRoster(afterRoster,settings,currentWeek);
    if(after.unfilled>baseline.unfilled)continue;
    const gain=round(after.value-baseline.value),cost=drop?dropCosts.get(id(drop)):{lost:0,depth:Infinity,complete:true};
    quick.push({add,drop,afterRoster,after,gain,dropCost:cost.lost,dropCostComplete:cost.complete,depthSurplus:cost.depth,screen:gain+(futureAddValue.get(id(add))||0)-cost.lost});
  }
  quick.sort((a,b)=>(mode==='week'?b.gain-a.gain:b.screen-a.screen)||Number(b.dropCostComplete)-Number(a.dropCostComplete)||a.dropCost-b.dropCost||b.depthSurplus-a.depthSurplus||id(a.add).localeCompare(id(b.add))||String(a.drop?.playerId).localeCompare(String(b.drop?.playerId)));
  const weeks=Array.from({length:Math.max(1,last-currentWeek+1)},(_,i)=>currentWeek+i);
  const scenarios=roster.some(injured)?['early','planning','late']:['planning'];
  const unknownReturn=roster.some(p=>injured(p)&&(!p.injuryEvidence||Date.parse(p.injuryEvidence.reviewBy)<=now));
  const before=new Map(scenarios.map(s=>[s,weeks.map(w=>weeklyRoster(roster,settings,w,s))]));
  const results=[];
  const screenedCounts=new Map(),screened=quick.filter(m=>{const n=screenedCounts.get(id(m.add))||0;screenedCounts.set(id(m.add),n+1);return n<2});
  // Reserve coverage for each position and material kicker near-ties before
  // filling the bounded search. High-projection WRs cannot erase all kickers.
  const priority=screened.filter(m=>m.add.position==='K'&&Math.abs(m.gain)<.5&&kickingComparison(m.add,m.drop,settings)?.material).slice(0,6);
  for(const position of ['QB','RB','WR','TE','K','D/ST'])priority.push(...screened.filter(m=>m.add.position===position).slice(0,4));
  const shortlist=[...new Set([...priority,...screened])].slice(0,48);
  for(const move of shortlist){
    const comparisons=scenarios.map(s=>({scenario:s,weeks:weeks.map((week,i)=>{
      const b=before.get(s)[i],a=weeklyRoster(move.afterRoster,settings,week,s);
      return {week,before:b.value,after:a.value,gain:round(a.value-b.value),complete:!b.missing&&!a.missing,coveragePreserved:a.unfilled<=b.unfilled,starters:a.starters};
    })}));
    const totals=comparisons.map(s=>round(s.weeks.filter(w=>w.complete).reduce((sum,w)=>sum+w.gain,0)));
    const selected=unknownReturn?totals.indexOf(Math.min(...totals)):scenarios.indexOf('planning');
    const planning=comparisons[selected],totalGain=totals[selected],complete=comparisons.every(s=>s.weeks.every(w=>w.complete));
    // A later bye is a quantified future cost, not a reason to erase a legal
    // this-week stream. Preserve current coverage and disclose future gaps.
    const coverage=comparisons.every(s=>s.weeks[0].coveragePreserved);
    const futureCoverageWeeks=[...new Set(comparisons.flatMap(s=>s.weeks.filter(w=>!w.coveragePreserved).map(w=>w.week)))];
    const kickerTie=move.add.position==='K'&&Math.abs(move.gain)<.5&&kickingComparison(move.add,move.drop,settings)?.material;
    const samePosition=roster.filter(p=>p.position===move.add.position&&!injured(p));
    const insurance=['RB','WR','TE'].includes(move.add.position)&&move.gain>=0&&move.gain<.5&&
      (!move.drop||move.drop.position==='QB'&&spareQuarterback)&&move.dropCost===0&&move.dropCostComplete&&
      finite(move.add.median)&&Number(move.add.median)>0&&move.add.hasEstimate!==false&&
      (samePosition.length<3||samePosition.some(p=>['Q','QUESTIONABLE'].includes(String(p.injuryStatus).toUpperCase())));
    if(!coverage||mode!=='week'&&!complete||mode==='week'&&move.gain<.5&&!kickerTie&&!insurance||mode!=='week'&&Math.max(...totals)<.5&&!kickerTie&&!insurance)continue;
    const evidence=opportunityEvidence(move.add,{now,week:currentWeek,season:settings.season}),tradeoff=Math.min(...totals)<0;
    const kind=tradeoff?'Short-term tradeoff':mode==='bridge'?'Four-week bridge':move.gain>=.5?'Starter upgrade':planning.weeks.some(w=>w.gain>=.5)?'Bye / future starter':'Stash review';
    const value=mode==='week'?move.gain:totalGain;
    const result={...move.add,mode,drop:move.drop,kind,lineupGain:move.gain,totalGain,waiverValue:value,
      horizonEstimate:round(mode==='week'?(move.add.median??0):healthyBaseline(move.add)),depthGain:null,seasonCost:null,
      comparison:planning.weeks,comparisonScenario:unknownReturn?'conservative return bookend':planning.scenario,futureCoverageWeeks,scenarioTotals:comparisons.map((s,i)=>({scenario:s.scenario,gain:totals[i]})),complete,tradeoff,
      evidence,method:WAIVER_METHOD,...deadline(move.add,roster.filter(p=>!move.drop||id(p)!==id(move.drop)),now,move.drop,slots,planning.weeks[0].starters.find(p=>p.playerId===id(move.add))?.slotId),
      actionPlan:waiverActionPlan(move.add,baseline,move.after,planning.weeks,settings),
      roleEvidence:waiverRoleSignals(move.add,settings.rolePlayers||freeAgents,settings),
      dropCost:move.dropCost,dropCostComplete:move.dropCostComplete,dropCostWeeks:lookahead,depthSurplus:move.depthSurplus,dropReplacement:move.drop?replacementByDrop.get(id(move.drop))||null:null,
      reasons:[`Compared move: add ${move.add.name}; ${move.drop?`drop ${move.drop.name}`:'use the open roster spot'}.`,
        `${move.gain>=0?'+':''}${move.gain.toFixed(1)} estimated starter points this week versus keeping your roster and using its best legal lineup.`,
        `Weeks ${currentWeek}–${last}: ${complete?totalGain.toFixed(1):'incomplete'} starter-point difference under the ${unknownReturn?'most conservative return bookend':planning.scenario} scenario. Future weeks use healthy-game baselines, not exact-week projections.`,
        'Equivalent current-lineup gains favor lower near-term starter loss, then surplus positional depth. Bench QB points are not compared directly with WR points.',...evidence.items],
      warnings:[...evidence.warnings,...(!complete?['Future schedule is incomplete; do not treat the partial total as rest-of-season value.']:[]),
        ...(tradeoff?['Some return scenarios favor keeping your current roster. This move needs manual review; do not sacrifice a stash blindly.']:[]),
        ...(futureCoverageWeeks.length?[`This move creates extra future starting-slot gaps in week(s) ${futureCoverageWeeks.join(', ')}. Those empty slots count as zero in the comparison; another move would be needed.`]:[]),
        ...(move.drop&&['QB','RB','WR','TE'].includes(move.drop.position)&&move.drop.percentOwned>=90?[`${move.drop.name} is rostered in at least 90% of ESPN leagues. Explore a trade before dropping this widely held player; lineup-only value does not measure trade-market value.`]:[]),
        'Reconsider if injury news, role, player ownership, projections or your budget changes. Future injuries and opponents are not predicted.'],
      alternatives:[],searchCoverage:{availablePlayers:candidates.length,pairs:quick.length,detailedPairs:shortlist.length}};
    result.review=reviewWaiverMove(result,roster,settings);
    result.kind=result.review.label;
    results.push(result);
  }
  results.sort((a,b)=>Number(b.review.spendingSupported)-Number(a.review.spendingSupported)||Number(a.tradeoff)-Number(b.tradeoff)||(mode==='week'?b.lineupGain-a.lineupGain:b.totalGain-a.totalGain)||b.totalGain-a.totalGain||a.dropCost-b.dropCost||b.depthSurplus-a.depthSurplus||id(a).localeCompare(id(b)));
  const unique=[];for(const result of results){const existing=unique.find(p=>id(p)===id(result));if(existing){if(existing.alternatives.length<2)existing.alternatives.push({drop:result.drop,lineupGain:result.lineupGain,waiverValue:result.waiverValue,dropCost:result.dropCost,dropCostComplete:result.dropCostComplete,dropCostWeeks:result.dropCostWeeks,futureCoverageWeeks:result.futureCoverageWeeks,dropReplacement:result.dropReplacement});}else unique.push(result);}
  for(const p of unique){
    const alternative=quick.filter(m=>id(m.add)!==id(p)&&m.add.position===p.position&&String(m.drop?.playerId??'')===String(p.drop?.playerId??'')&&m.add.isAvailable!==false&&m.gain>0&&m.after.starters.some(s=>s.playerId===id(m.add)))
      .sort((a,b)=>b.gain-a.gain||Number(b.add.status==='FREEAGENT')-Number(a.add.status==='FREEAGENT')||id(a.add).localeCompare(id(b.add)))[0];
    p.waiverAlternative=alternative?{playerId:id(alternative.add),name:alternative.add.name,status:alternative.add.status,lineupGain:alternative.gain,estimate:alternative.add.median,drop:p.drop?.name||null}:null;
    p.alternativeGain=alternative?round(p.lineupGain-alternative.gain):null;
    p.marketContext=waiverMarketContext(p,settings.market,now);
    p.faab=bidGuidance(p,settings.market,now);p.faabExplanation=p.faab.explanation;
  }
  const selected=unique.slice(0,limit);
  for(const position of ['QB','RB','WR','TE','K','D/ST']){const candidate=unique.find(p=>p.position===position);if(candidate&&!selected.some(p=>p.position===position)&&limit>=6){const replace=selected.findLastIndex(p=>selected.filter(q=>q.position===p.position).length>1);if(replace>=0)selected[replace]=candidate;}}
  return selected.map((p,i,all)=>({...p,claimPriority:i+1,fallbackClaims:all.filter(a=>id(a)!==id(p)&&a.position===p.position).slice(0,2).map(a=>({playerId:id(a),name:a.name,drop:a.drop?.name,bid:a.faab.available?`$${a.faab.low}–$${a.faab.high}`:null}))}));
}

export function injuryHolds(roster,freeAgents,settings){
  const {currentWeek=1,endWeek=17,slots={}}=settings;
  return roster.filter(injured).map(p=>{
    const irSlots=Number(slots[21]||0),occupied=roster.filter(r=>Number(r.lineupSlotId)===21).length;
    const evidence=p.injuryEvidence||null;
    const replacement=freeAgents.filter(a=>a.position===p.position&&!injured(a)&&healthyBaseline(a)!=null&&['FREEAGENT','WAIVERS'].includes(a.status)&&!Number(a.onTeamId||0)).sort((a,b)=>(healthyBaseline(b)||0)-(healthyBaseline(a)||0))[0];
    const gains=['early','planning','late'].map(s=>{
      let gain=0,complete=true;
      for(let w=currentWeek;w<=endWeek;w++){
        const hold=weeklyRoster(roster,settings,w,s),replace=weeklyRoster([...roster.filter(r=>id(r)!==id(p)),...(replacement?[{...replacement,isStarter:false,lineupSlotId:20}]:[])],settings,w,s);
        complete&&=!hold.missing&&!replace.missing;gain+=hold.value-replace.value;
      }
      return {scenario:s,holdGain:round(gain),complete};
    });
    const known=!!evidence&&Date.parse(evidence.reviewBy)>(settings.now||Date.now());
    return {player:p,replacement:replacement?{name:replacement.name,playerId:id(replacement)}:null,scenarios:gains,evidence,
      recommendation:p.cantCut?'ESPN currently prevents dropping':!known?'Hold pending return evidence':gains.every(g=>g.complete&&g.holdGain>0)?'Hold favored across scenarios':gains.every(g=>g.complete&&g.holdGain<0)?'Replacement favored; verify before dropping':'Hold / replace is scenario-dependent',
      ir:{slots:irSlots,occupied,open:Math.max(0,irSlots-occupied),eligible:(p.eligibleSlotIds||[]).includes(21)},
      explanation:`${irSlots?'Check ESPN IR eligibility and available slots.':'This league has no IR slots; the stash consumes a bench spot.'} Compare the named replacement with keeping this player through each return scenario. No drop is executed or automatically recommended.`,
      sourceCurrent:known};
  });
}
