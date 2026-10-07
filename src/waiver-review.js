// Decision checks are not point bonuses, win probabilities or calibrated skill.
import {playerIntelligence} from './intelligence.js';
const finite=v=>v!=null&&v!==''&&Number.isFinite(Number(v));
const avg=values=>values.length?values.reduce((a,b)=>a+b,0)/values.length:null;
export function validateKicking(k,metadata={}){
  const keys=['attempts','made','patAttempts','patMade','longAttempts','longMade','unknownDistances'];
  const counts=row=>keys.every(key=>Number.isInteger(row?.[key])&&row[key]>=0)&&row.made<=row.attempts&&row.patMade<=row.patAttempts&&row.longAttempts+row.unknownDistances<=row.attempts&&row.longMade<=row.longAttempts&&row.longMade<=row.made;
  if(!k||Number(k.season)!==Number(metadata.season??k.season)||!Array.isArray(k.weekly)||!k.weekly.length||k.weekly.length>22||k.games!==k.weekly.length||!counts(k)||!Number.isFinite(Date.parse(k.generatedAt)))throw new Error('Invalid kicking evidence');
  const seen=new Set();
  for(const row of k.weekly){if(!Number.isInteger(row.week)||row.week<1||row.week>Number(metadata.throughWeek??k.throughWeek)||row.team!==k.team||!row.eventId||seen.has(row.eventId)||!counts(row))throw new Error('Invalid kicking game evidence');seen.add(row.eventId);}
  if(keys.some(key=>k[key]!==k.weekly.reduce((sum,row)=>sum+row[key],0))||k.throughWeek!==Math.max(...k.weekly.map(row=>row.week)))throw new Error('Kicking aggregate disagrees with games');
  return k;
}
export function currentKicking(player,{currentWeek=1,season,now=Date.now()}={}){
  const k=player.feature?.opportunity?.kicking;
  const age=now-Date.parse(k?.generatedAt);
  if(!k||k.coverageComplete===false||k.team!==(player.team||player.proTeam)||season&&Number(k.season)!==Number(season)||
    Number(player.feature?.dataThroughWeek??k.datasetThroughWeek??k.throughWeek)<currentWeek-1||
    Number(k.throughWeek)>=currentWeek||!Number.isFinite(age)||age< -300000||age>8*86400000)return null;
  return k;
}
export function kickingEvidence(player,settings={}){
  const k=currentKicking(player,settings);
  if(!k)return {quality:'missing',items:[],warnings:['Current kicking opportunity is missing or stale; a projection is not a complete kicker review.']};
  const recent=(k.weekly||[]).filter(w=>w.week<settings.currentWeek).slice(-3);
  const count=recent.length,attempts=recent.reduce((n,w)=>n+w.attempts,0);
  return {quality:k.games>=3?'observed':'small-sample',throughWeek:k.throughWeek,source:k.source,
    items:[`${k.made}/${k.attempts} field goals and ${k.patMade}/${k.patAttempts} extra points across ${k.games} observed games.`,
      `${attempts} field-goal attempts across the latest ${count} observed games (${count?(attempts/count).toFixed(1):'unknown'}/game).`,
      `${k.longMade}/${k.longAttempts} from 50+ yards${k.unknownDistances?`; ${k.unknownDistances} attempt distances unknown`:''}.`,
      k.sample||'Observed kicking opportunities, not a point forecast.'],
    warnings:['Small samples, long kicks and perfect accuracy can regress; field-goal opportunities vary with game script.']};
}
export function kickingComparison(add,drop,settings={}){
  const a=currentKicking(add,settings),b=drop&&currentKicking(drop,settings);
  if(!a||!b)return null;
  const rate=k=>{const rows=k.weekly.filter(w=>w.week<settings.currentWeek).slice(-3);return {games:rows.length,rate:avg(rows.map(w=>w.attempts))};};
  const ar=rate(a),br=rate(b);
  if(ar.games<2||br.games<2)return null;
  return {addRate:ar.rate,holdRate:br.rate,addGames:ar.games,holdGames:br.games,
    material:Math.abs(ar.rate-br.rate)>=1,
    explanation:`Recent FG attempts/game: ${add.name} ${ar.rate.toFixed(1)} (${ar.games} games), ${drop.name} ${br.rate.toFixed(1)} (${br.games} games). Opportunity context only; no projected points added.`};
}
export function reviewWaiverMove(move,roster,settings={}){
  const {currentWeek=1,season,now=Date.now()}=settings,feature=move.feature,o=feature?.opportunity;
  const usage=(o?.weeklyUsage||[]).filter(w=>w.week<currentWeek&&w.team===(move.team||move.proTeam));
  const through=Number(feature?.dataThroughWeek??o?.throughWeek);
  const age=now-Date.parse(feature?.generatedAt);
  const current=!!o&&through>=currentWeek-1&&Number(o.throughWeek)<currentWeek&&
    (!season||Number(feature.season)===Number(season))&&Number.isFinite(age)&&age>=-300000&&age<=8*86400000;
  const kicking=move.position==='K'?kickingComparison(move,move.drop,settings):null;
  const observed=move.position==='K'?!!currentKicking(move,settings)&&currentKicking(move,settings).games>=3:current&&usage.length>=3;
  const intelligence=playerIntelligence(move,{season,week:currentWeek,now});
  const practice=intelligence.practice;
  const conflict=intelligence.warnings.some(w=>/Sources disagree/.test(w));
  const uncertain=['Q','QUESTIONABLE'].includes(String(move.injuryStatus).toUpperCase())||conflict;
  const usageWeeks=new Set(usage.map(w=>Number(w.week)));
  const points=current?(o.weeklyPoints||[]).filter(w=>w.week<currentWeek&&usageWeeks.has(Number(w.week))&&finite(w.points)).slice(-4).map(w=>Number(w.points)):[];
  const projection=Number(move.projectedPoints),poor=points.length>=3&&projection>0&&points.every(v=>v<projection*.75);
  const growth=(move.roleEvidence?.signals||[]).some(s=>['usage-rise','scoring-role'].includes(s.kind));
  const benefit=move.mode==='week'?move.lineupGain:move.totalGain;
  const starts=move.actionPlan?.startWeeks||[];
  const close=move.lineupGain<.5&&move.lineupGain>=-.5;
  const missing=[];
  if(!observed)missing.push(move.position==='K'?'At least three current observed kicking games.':'Current-season role history through the latest completed week.');
  if(['WR','TE','RB'].includes(move.position)&&!intelligence.routes)missing.push('Receiving-route evidence (where the source supports this position).');
  if(['QB','RB','WR','TE'].includes(move.position)&&(!practice||intelligence.warnings.some(w=>/Practice evidence is stale/.test(w))))missing.push('A current official practice/game-status report; absence is not clearance.');
  if(move.position==='K'&&!move.weather?.indoor&&!move.game?.indoor&&!move.weather)missing.push('Current outdoor kickoff weather.');
  if(move.position==='K')missing.push('Independent upcoming scoring and red-zone matchup assessment; kicking volume alone does not establish it.');
  const counter=[];
  if(close)counter.push('The weekly point estimates are effectively tied; do not turn a small decimal difference into a confident upgrade.');
  if(poor&&!growth)counter.push(`Each of the last ${points.length} observed scores was below 75% of the current ESPN projection. No observed workload-growth signal establishes a rebound.`);
  if(o?.touchdownDependent)counter.push('Recent touchdowns came on limited opportunities; repeatable volume is not established.');
  if(move.tradeoff)counter.push('At least one return scenario favors holding your roster.');
  if(!starts.length)counter.push('No starting week is established in this comparison; this is only optional bench insurance.');
  if(uncertain)counter.push('Availability needs confirmation before committing a starting slot or paid bid.');
  if(move.drop)counter.push(`Dropping ${move.drop.name} gives up their depth and future options; reacquisition is not guaranteed.`);
  if(move.position==='K')counter.push('Kicker opportunities and long-distance conversion can regress; weather and game script can reverse the preference.');
  if(move.position==='D/ST')counter.push('Defensive touchdowns and turnovers are volatile; a favorable projection is not a stable weekly floor.');
  if(!counter.length)counter.push('Forecast error and subsequent role changes can erase the estimated benefit.');
  const supported=observed&&!uncertain&&!(poor&&!growth)&&!o?.touchdownDependent&&!move.tradeoff&&benefit>=2&&starts.length>0&&!close;
  const category=conflict?'availability-review':poor&&!growth?'watch':!starts.length?'insurance':close?'close-call':supported?'supported-upgrade':'review-needed';
  const labels={'availability-review':'Availability check required',watch:'Hold / watch — rebound unproven',insurance:'Optional bench insurance','close-call':'Close projection — review evidence','supported-upgrade':'Evidence-supported upgrade','review-needed':'Review before acting'};
  return {version:1,category,label:labels[category],spendingSupported:!!supported,evidenceConfidence:observed?'Observed sample':'Incomplete evidence',
    forecastConfidence:'Not a calibrated probability',missing,counterarguments:counter,kicking,
    baseline:`Keep your roster and use its best legal lineup; modeled ${move.mode==='week'?'weekly':'window'} difference is ${Number(benefit).toFixed(1)} points.`,
    whyNow:growth?'Recent workload/scoring-role evidence merits review.':kicking?.material?kicking.explanation:poor?'No verified catalyst for the projected rebound.':'The modeled lineup comparison—not an assumed breakout—is the basis for considering this move.',
    scope:starts.length?`Conditional starting weeks: ${starts.join(', ')}.`:'No modeled starting week; do not pay a starter-upgrade price.',
    alternatives:[{kind:'hold',label:'Keep current roster',gain:0},{kind:'move',label:move.drop?`Add ${move.name}; drop ${move.drop.name}`:`Add ${move.name}`,gain:benefit}],
    checkedAt:new Date(now).toISOString(),critical:conflict,
    reviewTriggers:['Recheck practice and active status before the relevant kickoff.','Recheck role, teammate returns, ownership and acquisition cost.','If the expected starting opportunity disappears, reassess holding rather than automatically churn.']};
}

export function waiverReviewCoverage(freeAgents,plans){
  return ['QB','RB','WR','TE','K','D/ST'].map(position=>({position,
    loaded:freeAgents.filter(p=>p.position===position).length,
    comparisons:plans.filter(p=>p.position===position).length,
    supported:plans.filter(p=>p.position===position&&p.review?.spendingSupported).length}));
}
