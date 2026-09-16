import {optimizeLineup,eligibleForSlot} from './analysis.js';

export const WAIVER_METHOD='waiver-plan-v3';
const id=p=>String(p.playerId), round=n=>Math.round(n*10)/10;
const finite=v=>v!=null&&Number.isFinite(Number(v));
const injured=p=>['OUT','IR','INJURY_RESERVE','DOUBTFUL','SUSPENDED','SUSPENSION'].includes(String(p.injuryStatus).toUpperCase());
const locked=(p,now)=>p.game?.kickoff&&Date.parse(p.game.kickoff)<=now;

// This is a healthy-game baseline, never a promise of availability or recovery.
export function healthyBaseline(p){
  if(p.team==='FA'||p.proTeam==='FA')return null;
  const history=finite(p.feature?.seasonPpg)?Number(p.feature.seasonPpg):null;
  if(injured(p)&&history!=null)return Math.max(0,history);
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
    const value=week===currentWeek?(finite(p.median)?Number(p.median):null):healthyBaseline(p);
    if(available&&value==null)missing++;
    return {...p,decisionScore:value??0,hasEstimate:value!=null,isAvailable:available,
      ...(week!==currentWeek?{game:null,isStarter:false}:{}),planValue:value??0};
  });
  const lineup=optimizeLineup(players,slots,week===currentWeek?players:[],week===currentWeek?now:0);
  return {week,value:round(lineup.starters.reduce((sum,p)=>sum+p.planValue,0)),missing,
    unfilled:lineup.unfilledSlots.length,starters:lineup.starters.map(p=>({playerId:id(p),name:p.name,slotId:p.assignedSlotId,estimate:round(p.planValue)}))};
}

export function opportunityEvidence(p){
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
  return {quality:o.games>=3?'observed':'small-sample',items,warnings,throughWeek:o.throughWeek,source:o.source};
}

export function bidGuidance(move,market={},now=Date.now()){
  const {budget,remaining,minimumBid,type,verifiedAt,winningBids=[]}=market;
  if(type!=='FAAB'||![budget,remaining,minimumBid].every(finite)||!Number.isFinite(Date.parse(verifiedAt))||now-Date.parse(verifiedAt)>300000)
    return {available:false,explanation:'No bid: a fresh FAAB budget, remaining balance and minimum bid are required. Check ESPN league settings.'};
  const min=Math.max(0,Math.ceil(minimumBid)),balance=Math.max(0,Math.floor(remaining));
  if(balance<min)return {available:false,explanation:'Remaining FAAB is below the league minimum bid.'};
  // An explicit spending policy, not fitted winning odds or a dollar value of points.
  const fraction=move.lineupGain>=5?.08:move.lineupGain>=2?.04:move.totalGain>5?.02:.01;
  const cap=Math.min(balance,Math.max(min,Math.floor(budget*fraction)));
  const samples=winningBids.filter(b=>b.position===move.position&&finite(b.amount)&&b.amount>=0).map(b=>Number(b.amount)).sort((a,b)=>a-b);
  const median=samples.length>=5?samples[Math.floor(samples.length/2)]:null;
  const low=Math.min(cap,Math.max(min,Math.floor(cap*.5))),high=Math.min(cap,Math.max(low,median??cap));
  return {available:true,low,high,max:cap,remaining:balance,minimumBid:min,sampleSize:samples.length,marketMedian:median,
    explanation:`Budget policy: ${Math.round(fraction*100)}% of the original $${budget} budget, capped at $${balance} remaining. ${median==null?'Not enough verified comparable winning bids; no market estimate.':`Historical median $${median} across ${samples.length} position-matched claims, not winning odds.`} Do not exceed $${cap}; alternatives are mutually exclusive, not a combined shopping list.`};
}

function deadline(add,roster,now,drop){
  const backups=roster.filter(p=>!p.isStarter&&!injured(p)&&p.eligibleForRecommendation!==false&&p.game?.kickoff&&Date.parse(p.game.kickoff)>now&&
    (add.eligibleSlotIds||[]).some(slot=>slot!==20&&slot!==21&&eligibleForSlot(p,slot))).sort((a,b)=>(b.median??0)-(a.median??0));
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
  // Screen all legal pairs first; the most promising 48 moves receive the full
  // multiweek/scenario comparison. The UI discloses this bounded search.
  for(const add of candidates)for(const drop of drops){
    const afterRoster=[...roster.filter(p=>!drop||id(p)!==id(drop)),{...add,isStarter:false,lineupSlotId:20}];
    const after=weeklyRoster(afterRoster,settings,currentWeek);
    if(after.unfilled>baseline.unfilled)continue;
    const gain=round(after.value-baseline.value),upside=healthyBaseline(add)-(drop?healthyBaseline(drop)||0:0);
    quick.push({add,drop,afterRoster,gain,screen:mode==='week'?gain+Math.max(-2,Math.min(2,upside*.1)):upside+gain});
  }
  quick.sort((a,b)=>b.screen-a.screen||id(a.add).localeCompare(id(b.add))||String(a.drop?.playerId).localeCompare(String(b.drop?.playerId)));
  const last=mode==='bridge'?Math.min(endWeek,currentWeek+3):endWeek;
  const weeks=Array.from({length:Math.max(1,last-currentWeek+1)},(_,i)=>currentWeek+i);
  const scenarios=roster.some(injured)?['early','planning','late']:['planning'];
  const before=new Map(scenarios.map(s=>[s,weeks.map(w=>weeklyRoster(roster,settings,w,s))]));
  const results=[];
  const screenedCounts=new Map(),shortlist=quick.filter(m=>{const n=screenedCounts.get(id(m.add))||0;screenedCounts.set(id(m.add),n+1);return n<2}).slice(0,48);
  for(const move of shortlist){
    const comparisons=scenarios.map(s=>({scenario:s,weeks:weeks.map((week,i)=>{
      const b=before.get(s)[i],a=weeklyRoster(move.afterRoster,settings,week,s);
      return {week,before:b.value,after:a.value,gain:round(a.value-b.value),complete:!b.missing&&!a.missing,coveragePreserved:a.unfilled<=b.unfilled,starters:a.starters};
    })}));
    const planning=comparisons.find(s=>s.scenario==='planning'),complete=planning.weeks.every(w=>w.complete),coverage=comparisons.every(s=>s.weeks.every(w=>w.coveragePreserved));
    const totals=comparisons.map(s=>round(s.weeks.filter(w=>w.complete).reduce((sum,w)=>sum+w.gain,0))),totalGain=totals[scenarios.indexOf('planning')];
    if(!coverage||mode!=='week'&&!complete||mode==='week'&&move.gain<.5||mode!=='week'&&Math.max(...totals)<.5)continue;
    const evidence=opportunityEvidence(move.add),tradeoff=Math.min(...totals)<0;
    const kind=tradeoff?'Short-term tradeoff':mode==='bridge'?'Four-week bridge':move.gain>=.5?'Starter upgrade':planning.weeks.some(w=>w.gain>=.5)?'Bye / future starter':'Stash review';
    const value=mode==='week'?move.gain:totalGain;
    const result={...move.add,mode,drop:move.drop,kind,lineupGain:move.gain,totalGain,waiverValue:value,
      horizonEstimate:round(mode==='week'?(move.add.median??0):healthyBaseline(move.add)),depthGain:null,seasonCost:null,
      comparison:planning.weeks,scenarioTotals:comparisons.map((s,i)=>({scenario:s.scenario,gain:totals[i]})),complete,tradeoff,
      evidence,method:WAIVER_METHOD,...deadline(move.add,roster.filter(p=>!move.drop||id(p)!==id(move.drop)),now,move.drop),
      reasons:[`Add ${move.add.name}; ${move.drop?`drop ${move.drop.name}`:'use the open roster spot'}.`,
        `${move.gain>=0?'+':''}${move.gain.toFixed(1)} estimated starter points this week versus keeping your roster and using its best legal lineup.`,
        `Weeks ${currentWeek}–${last}: ${complete?totalGain.toFixed(1):'incomplete'} starter-point difference under the planning scenario. Future weeks use healthy-game baselines, not exact-week projections.`,...evidence.items],
      warnings:[...evidence.warnings,...(!complete?['Future schedule is incomplete; do not treat the partial total as rest-of-season value.']:[]),
        ...(tradeoff?['Some return scenarios favor keeping your current roster. This move needs manual review; do not sacrifice a stash blindly.']:[]),
        'Reconsider if injury news, role, player ownership, projections or your budget changes. Future injuries and opponents are not predicted.'],
      alternatives:[],searchCoverage:{availablePlayers:candidates.length,pairs:quick.length,detailedPairs:shortlist.length}};
    result.faab=bidGuidance(result,settings.market,now);result.faabExplanation=result.faab.explanation;
    results.push(result);
  }
  results.sort((a,b)=>Number(a.tradeoff)-Number(b.tradeoff)||(mode==='week'?b.lineupGain-a.lineupGain:b.totalGain-a.totalGain)||b.totalGain-a.totalGain||id(a).localeCompare(id(b)));
  const unique=[];for(const result of results){const existing=unique.find(p=>id(p)===id(result));if(existing){if(existing.alternatives.length<2)existing.alternatives.push({drop:result.drop,lineupGain:result.lineupGain,waiverValue:result.waiverValue});}else unique.push(result);}
  return unique.slice(0,limit).map((p,i,all)=>({...p,claimPriority:i+1,fallbackClaims:all.filter(a=>id(a)!==id(p)&&a.position===p.position).slice(0,2).map(a=>({playerId:id(a),name:a.name,drop:a.drop?.name,bid:a.faab.available?`$${a.faab.low}–$${a.faab.high}`:null}))}));
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
