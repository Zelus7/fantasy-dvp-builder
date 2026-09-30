// Context-only evidence: no uncalibrated multipliers or inferred return dates.
const finite=v=>v!=null&&Number.isFinite(Number(v));
const age=(value,now)=>{const t=Date.parse(value);return Number.isFinite(t)&&t<=now+300000?Math.max(0,now-t):Infinity};
const limits={routes:7*86400000,practice:12*3600000,injuries:12*3600000};
export function evidenceCurrent(row,kind,{season,week,now=Date.now()}={}){
  if(!row||Number(row.season)!==Number(season)||age(row.fetchedAt,now)>limits[kind])return false;
  if(kind==='practice')return Number(row.week)===Number(week);
  return age(row.sourceUpdatedAt,now)<=limits[kind];
}
export function intelligenceCoverage(metadata={},players=[],lookup={},context={}){
  const sources={};
  for(const [kind,label] of [['routes','receivingUsage'],['practice','practiceReports'],['injuries','injuryReports']]){
    const feed=metadata[kind],eligible=players.filter(p=>kind==='routes'?p.position==='WR':['QB','RB','WR','TE'].includes(p.position));
    const matched=eligible.filter(p=>lookup[String(p.playerId)]?.opportunity?.[kind]).length;
    const current=feed?.status==='available'&&evidenceCurrent(feed,kind,context);
    let coverage=kind==='routes'?`${matched}/${eligible.length} loaded WRs matched. Public season-to-date routes, targets per route, yards per route and target depth. Weekly routes, route participation and RB/TE routes are unavailable; no inferred weekly totals.`:
      kind==='practice'?`${feed?.teamsWithReports??0}/32 teams have published player rows; ${matched} loaded players listed. Official W${feed?.week??context.week} practice/game designations as observed at fetch time; the page does not identify the practice day. Absence is not medical clearance. Not a confirmed-inactive feed.`:
      `${matched} loaded players have source-linked ESPN injury reports. Feed refresh and individual report dates differ. Not a complete practice/inactive feed; reported return dates are not used as recovery predictions.`;
    if(feed?.status!=='available')coverage=`Latest fetch unavailable. ${coverage}`;
    sources[label]={status:!feed||feed.status!=='available'?'missing':current?'partial':'stale',
      updatedAt:kind==='practice'?feed?.fetchedAt:feed?.sourceUpdatedAt,
      fetchedAt:feed?.fetchedAt||null,sourceUrl:feed?.sourceUrl||null,coverage};
  }
  return sources;
}
export function playerIntelligence(player,{now=Date.now(),season,week}={}){
  const o=player.feature?.opportunity||{},context={now,season:season??player.feature?.season,week:week??player.game?.week},items=[],warnings=[];
  const routes=o.routes,practice=o.practice,injuries=o.injuries;
  if(routes){
    if(evidenceCurrent(routes,'routes',context)){
      items.push(`${routes.routesRun} routes run, season-to-date (${routes.source}; updated ${routes.sourceUpdatedAt}).`);
      if(finite(routes.targetsPerRouteRun))items.push(`${(routes.targetsPerRouteRun*100).toFixed(1)}% targets per route; this measures targets earned on routes, not route participation.`);
      if(finite(routes.yardsPerRouteRun))items.push(`${Number(routes.yardsPerRouteRun).toFixed(2)} receiving yards per route.`);
      if(finite(routes.averageDepthOfTarget))items.push(`${Number(routes.averageDepthOfTarget).toFixed(1)} yards average target depth.`);
    }else warnings.push('Receiving-route evidence is stale or belongs to a different season; do not use it as current usage.');
  }else if(['WR','TE','RB'].includes(player.position))warnings.push('Current route evidence is unavailable for this player; snap share is not route participation.');
  if(practice){
    if(evidenceCurrent(practice,'practice',context)){
      items.push(`Official Week ${practice.week} report: ${practice.practiceStatus||'practice participation not stated'}; game status ${practice.gameStatus||'not designated'}.`);
      if(/Did Not|Limited/.test(practice.practiceStatus||''))warnings.push('Limited/missed practice needs follow-up before lineup lock; it does not by itself mean the player is out.');
      if(/^(Out|Doubtful)$/i.test(practice.gameStatus||''))warnings.push(`Sources disagree or need confirmation: the official Week ${practice.week} game designation is ${practice.gameStatus}. Verify this slot before acting on a projection.`);
    }else warnings.push('Practice evidence is stale or from a different week; verify the latest official report.');
  }
  if(injuries){
    items.push(`ESPN public injury report: ${injuries.status}; report dated ${injuries.reportedAt||'unknown'}.`);
    if(!evidenceCurrent(injuries,'injuries',context))warnings.push('Public injury feed is stale; retain ESPN fantasy status and verify availability.');
    else if(age(injuries.reportedAt,now)>3*86400000)warnings.push('The individual injury report is older than three days even though the feed was checked recently.');
    const reported=String(injuries.status).toUpperCase().replaceAll(' ','_'),fantasy=String(player.injuryStatus||'').toUpperCase();
    if(evidenceCurrent(injuries,'injuries',context)&&['OUT','INJURED_RESERVE','SUSPENDED'].includes(reported)&&['ACTIVE','NORMAL'].includes(fantasy))warnings.push('Sources disagree on availability. Verify ESPN and the latest official report before acting.');
  }
  return {items,warnings,routes,practice,injuries,contextOnly:true};
}
