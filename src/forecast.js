// Pure forecast boundary shared by the Worker, browser and immutable replays.
import {normalizeTeamAbbreviation} from './constants.js';
const finite=n=>typeof n==='number'&&Number.isFinite(n);
const hard=p=>['OUT','IR','INJURY_RESERVE','DOUBTFUL','SUSPENDED','SUSPENSION'].includes(String(p.injuryStatus).toUpperCase());
export function validateForecast(f,metadata={}){
  if(f==null)return null;
  if(typeof f!=='object'||!/^opportunity-v1:[a-f0-9]{16}$/.test(f.version||'')||
    !Number.isInteger(f.season)||f.season<2026||!Number.isInteger(f.targetWeek)||f.targetWeek<1||f.targetWeek>18||
    f.throughWeek!==f.targetWeek-1||f.conditionalOnAppearance!==true||!Number.isInteger(f.historyGames)||f.historyGames<3||
    !Number.isInteger(f.currentGames)||f.currentGames<0||f.currentGames>18||
    !Number.isInteger(f.lastObservedWeek)||f.lastObservedWeek<1||f.lastObservedWeek>18||
    ![f.season,f.season-1].includes(f.lastObservedSeason)||
    f.lastObservedSeason===f.season&&f.lastObservedWeek>=f.targetWeek||
    metadata.season!=null&&f.season!==Number(metadata.season)||
    metadata.scoringHash!=null&&f.scoringHash!==metadata.scoringHash||
    JSON.stringify(f).length>6000)throw new Error('Invalid forecast metadata');
  if(!f.horizons?.['1'])throw new Error('Missing weekly forecast');
  for(const [key,h] of Object.entries(f.horizons)){
    if(!['1','4'].includes(key)||key==='4'&&f.targetWeek>15||
      !h||![h.points,h.targets,h.carries,h.low,h.high,h.measuredCoverage,h.mae,h.baselineMae].every(finite)||
      h.points < -10||h.points>65||h.targets<0||h.targets>50||h.carries<0||h.carries>50||
      h.low>h.high||h.low < -100||h.high>150||h.nominalCoverage!==.8||h.measuredCoverage<0||h.measuredCoverage>1||
      !Number.isInteger(h.calibrationCount)||h.calibrationCount<100||!Number.isInteger(h.holdoutCount)||h.holdoutCount<100||
      typeof h.approvedFallback!=='boolean')throw new Error('Invalid forecast horizon');
  }
  return f;
}

export function forecastFor(player,context={}){
  const f=player.feature?.forecast;
  try{validateForecast(f);}catch{return null;}
  if(!f||!context.currentWeek||!context.season||f.season!==context.season||f.targetWeek!==context.currentWeek)return null;
  const generated=Date.parse(player.feature?.generatedAt||''),now=context.now??Date.now();
  if(!Number.isFinite(generated)||now-generated>36*3600000||generated>now+300000)return null;
  const changedTeam=f.historicalTeam&&normalizeTeamAbbreviation(player.team||player.proTeam)!==normalizeTeamAbbreviation(f.historicalTeam);
  const inactive=hard(player)||player.isAvailable===false;
  const gap=f.lastObservedSeason===f.season?f.targetWeek-f.lastObservedWeek-1:Math.max(0,f.targetWeek-1);
  return {...f,usable:!inactive&&!changedTeam&&gap<=2,
    warnings:[...(inactive?['Conditional on playing: not a projection of points while unavailable.']:[]),
      ...(changedTeam?['Team changed since the last observed game; historical role may not transfer.']:[]),
      ...(gap>2?['Long gap since observed usage; forecast is not used for planning.']:[]),
      ...(f.currentGames<3?['Small current-season sample; prior-season usage remains influential.']:[])]};
}

export function learnedFallback(player,horizon=1){
  const f=player.forecast,h=f?.horizons?.[String(horizon)];
  return f?.usable&&h?.approvedFallback===true?h.points:null;
}

export function forecastAccuracy(players,actuals,week,now){
  const seen=new Set(),rows=[];
  if(!finite(now))return {count:0,status:'awaiting-matched-scores'};
  for(const p of players){
    const id=String(p.playerId),f=p.forecast,h=f?.horizons?.['1'];
    if(seen.has(id)||!f?.usable||f.targetWeek!==week||!h)continue;
    seen.add(id);
    if(!p.game?.kickoff||!Number.isFinite(Date.parse(p.game.kickoff))||Date.parse(p.game.kickoff)<=now||!finite(actuals[id]))continue;
    if(!finite(p.projectedPoints))continue; // Matched cohort for a fair comparison.
    rows.push({prediction:h.points,espn:p.projectedPoints,actual:actuals[id],low:h.low,high:h.high});
  }
  if(!rows.length)return {count:0,status:'awaiting-matched-scores'};
  const avg=fn=>Math.round(rows.reduce((s,r)=>s+fn(r),0)/rows.length*100)/100;
  return {count:rows.length,status:'scored',modelMae:avg(r=>Math.abs(r.prediction-r.actual)),
    espnMae:avg(r=>Math.abs(r.espn-r.actual)),modelBias:avg(r=>r.prediction-r.actual),
    intervalCoverage:avg(r=>Number(r.actual>=r.low&&r.actual<=r.high)),
    caveat:'Same players, predictions frozen before their kickoffs. Actual zeroes may include non-appearances; this is a live decision audit, not conditional-on-playing calibration.'};
}
