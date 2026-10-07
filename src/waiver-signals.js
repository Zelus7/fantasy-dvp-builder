const id=p=>String(p.playerId),finite=v=>v!=null&&Number.isFinite(Number(v));
const unavailable=p=>['OUT','IR','INJURY_RESERVE','DOUBTFUL','SUSPENDED','SUSPENSION'].includes(String(p.injuryStatus).toUpperCase());

// These are auditable research triggers, never extra projected fantasy points.
export function waiverRoleSignals(player,players=[],{currentWeek=1,season,now=Date.now()}={}){
  const signals=[],warnings=[],o=player.feature?.opportunity;
  const current=o&&Number(o.throughWeek)===currentWeek-1&&(!season||Number(player.feature?.season)===Number(season));
  const usage=current?(o.weeklyUsage||[]).filter(w=>Number(w.week)<currentWeek&&w.team===(player.team||player.proTeam)).sort((a,b)=>a.week-b.week):[];
  const last=usage.at(-1),prior=usage.slice(-4,-1);
  const average=key=>{const values=prior.map(w=>w[key]).filter(finite).map(Number);return values.length>=2?values.reduce((s,v)=>s+v,0)/values.length:null;};
  if(last&&last.week===currentWeek-1){
    for(const [key,threshold,label] of [['targets',3,'targets'],['carries',5,'carries'],['snapShare',.15,'snap share']]){
      const baseline=average(key);
      if(baseline!=null&&finite(last[key])&&last[key]-baseline>=threshold){
        const format=v=>key==='snapShare'?`${Math.round(v*100)}%`:Number(v).toFixed(1);
        signals.push({kind:'usage-rise',label:`Week ${last.week}: ${format(last[key])} ${label}, versus ${format(baseline)} over the prior ${prior.filter(w=>finite(w[key])).length} observed games.`,week:last.week});
      }
    }
    if(finite(last.redZoneOpportunities)&&last.redZoneOpportunities>=3)signals.push({kind:'scoring-role',label:`${last.redZoneOpportunities} carries + targets inside the 20 in Week ${last.week}.`,week:last.week});
  }
  if(o&&!current)warnings.push('Historical usage is not through the latest completed week; it cannot establish a new role.');
  if(o?.touchdownDependent)warnings.push('Recent touchdowns came on limited touches; do not project the scoring burst forward.');
  // A missing starter creates a question, not a guarantee that this candidate inherits touches.
  const vacancies=players.filter(p=>id(p)!==id(player)&&(p.team||p.proTeam)===(player.team||player.proTeam)&&p.position===player.position&&unavailable(p)&&
    (Number(p.feature?.opportunity?.depthPosition)===1||Number(p.percentOwned)>=40||Number(p.feature?.opportunity?.carryShare)>=.3||Number(p.feature?.opportunity?.targetShare)>=.15));
  for(const p of vacancies.slice(0,3))signals.push({kind:'injury-opening',playerId:id(p),label:`${p.name} is ${p.injuryStatus}. Verify who inherits the ${player.position} work; this does not establish ${player.name} as the replacement.`});
  if(vacancies.length)warnings.push('Same-team injuries identify a research opportunity, not a depth-chart promotion or a medical return forecast.');
  return {signals,warnings,currentUsage:!!current,checkedAt:new Date(now).toISOString(),contextOnly:true};
}

export function emergingWaiverTargets(freeAgents,roster,settings={}){
  const owned=new Set(roster.map(id)),rolePlayers=settings.rolePlayers||freeAgents;
  return freeAgents.filter(p=>!owned.has(id(p))&&!Number(p.onTeamId||0)&&['WAIVERS','FREEAGENT'].includes(p.status)&&['RB','WR','TE'].includes(p.position)&&!unavailable(p)&&
    (p.team||p.proTeam)!=='FA'&&!(p.game?.kickoff&&Date.parse(p.game.kickoff)<=(settings.now??Date.now())))
    .map(p=>({...p,roleEvidence:waiverRoleSignals(p,rolePlayers,settings)})).filter(p=>p.roleEvidence.signals.length)
    .sort((a,b)=>b.roleEvidence.signals.length-a.roleEvidence.signals.length||id(a).localeCompare(id(b)))
    .slice(0,settings.signalLimit??12).map(p=>({...p,kind:'Role watch — not a claim recommendation',
      nextCheck:'Verify the latest practice/designation, depth competition and projected workload before committing FAAB.',
      exitTrigger:'Reassess when the injured teammate returns, a competing back/receiver is activated, or next-game snaps and opportunities do not persist.'}));
}

export function waiverActionPlan(add,before,after,comparison,settings={}){
  const selected=after.starters.find(p=>id(p)===id(add)),retained=new Set(after.starters.map(id));
  const displaced=before.starters.filter(p=>!retained.has(id(p)));
  const startWeeks=comparison.filter(w=>w.complete&&w.starters.some(p=>id(p)===id(add))).map(w=>w.week);
  const nextWeek=comparison.find(w=>w.week>settings.currentWeek&&!w.starters.some(p=>id(p)===id(add)));
  return {startsNow:!!selected,startSlot:selected?.slotId??null,displaced:displaced.map(p=>({playerId:id(p),name:p.name,slotId:p.slotId})),startWeeks,
    duration:startWeeks.length?`Starts in the displayed scenario in week(s) ${startWeeks.join(', ')}. These are conditional lineup selections, not a promised role duration.`:'No confirmed starting week in the loaded comparison.',
    nextMove:nextWeek?`Before Week ${nextWeek.week}, compare holding ${add.name} with your returning players and next week's waiver pool; no future pickup is assumed.`:'Recheck after the next game and whenever teammate availability changes; no automatic follow-up drop.',
    reviewTriggers:['A teammate returns or another player takes over the role.','The latest snaps, targets or carries fail to sustain the opportunity.','Your roster, projections, bye coverage or acquisition cost changes.']};
}
