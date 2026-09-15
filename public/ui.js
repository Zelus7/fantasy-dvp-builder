export const esc=value=>String(value??'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
export const number=value=>value!=null&&Number.isFinite(Number(value))?Number(value).toFixed(1):'—';
export const actualNumber=value=>value!=null&&Number.isFinite(Number(value))?Number(value).toFixed(2):'—';
export const signed=value=>value==null?'—':`${Number(value)>=0?'+':''}${number(value)}`;
export const date=value=>value&&Number.isFinite(Date.parse(value))?new Date(value).toLocaleString():'Not available';
export const empty=message=>`<div class="empty">${esc(message)}</div>`;
export const warnings=items=>(items||[]).map(w=>`<div class="warning">${esc(w)}</div>`).join('');
export const detail=(title,body)=>`<details><summary>${esc(title)}</summary><div class="detail-body">${body}</div></details>`;
export const head=(eyebrow,title,description='')=>`<div class="section-head"><div><div class="eyebrow">${esc(eyebrow)}</div><h2>${esc(title)}</h2>${description?`<p class="muted">${esc(description)}</p>`:''}</div></div>`;
export const slot=id=>({0:'QB',1:'TQB',2:'RB',3:'RB/WR',4:'WR',5:'WR/TE',6:'TE',7:'OP',16:'D/ST',17:'K',20:'BE',21:'IR',23:'FLEX'}[id]||`Slot ${id}`);
export const metric=(label,value)=>`<div><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`;
export const metricHelp=()=>detail('What do these numbers mean?',`<p><strong>Actual</strong> is the selected week's ESPN score, not a prediction. A dash means unavailable; a real zero stays zero.</p><p><strong>ESPN projection</strong> is ESPN's exact-week forecast. <strong>Estimate</strong> uses that projection unchanged when available; otherwise it uses an experimental historical baseline. The former “median” label meant this central estimate, not a measured statistical median.</p><p><strong>Low / high scenarios</strong> are heuristic ranges, not calibrated probability bounds. <strong>Lineup rank score</strong> weights the low, central and high scenarios using your risk settings. It is not an actual score.</p>`);

export function playerIdentity(p,teams={}) {
  const team=teams[p.team||p.proTeam],abbr=p.team||p.proTeam||'FA';
  return `<div class="identity team-${esc(abbr)}">${team?.logo?`<img class="team-logo" src="${esc(team.logo)}" alt="${esc(team.name)}" loading="lazy" width="36" height="36">`:''}<div><strong>${esc(p.name)}</strong><small>${esc(p.position)} · ${esc(abbr)}${p.opponent?` vs ${esc(p.opponent)}`:p.scheduleUnavailable?' · Schedule unknown':p.isAvailable===false&&p.injuryStatus==='ACTIVE'?' · BYE':''}</small></div></div>`;
}

export function preferenceButtons(p,prefs={},owned=false) {
  const watched=(prefs.watchlistIds||[]).includes(String(p.playerId)),protectedPlayer=(prefs.protectedIds||[]).includes(String(p.playerId));
  return `<div class="button-row"><button class="secondary small" data-watch="${esc(p.playerId)}" aria-pressed="${watched}">${watched?'✓ Watching':'Watch player'}</button>${owned?`<button class="secondary small" data-protect="${esc(p.playerId)}" aria-pressed="${protectedPlayer}">${protectedPlayer?'✓ Protected':'Protect from moves'}</button>`:''}</div>`;
}

export function playerCard(p,teams={},prefs={},owned=false) {
  return `<article class="card player team-${esc(p.team||p.proTeam)}">${playerIdentity(p,teams)}<div class="pill">${esc(p.injuryStatus||'Status unknown')} · ${esc(p.matchupLabel||'No matchup data')}</div><div class="metric-grid">${metric('Actual',actualNumber(p.actualPoints))}${metric('ESPN projection',number(p.projectedPoints))}${metric('Estimate',number(p.median))}</div>${detail('Evidence & limitations',`<p>${esc(p.estimateSource||'Source not available')}</p><p>Low / high scenarios: ${number(p.floor)} / ${number(p.ceiling)}. Lineup rank score: ${p.hasEstimate?number(p.decisionScore):'—'}.</p>${(p.reasons||[]).map(r=>`<p>${esc(r)}</p>`).join('')}${warnings(p.warnings)}`)}${preferenceButtons(p,prefs,owned)}</article>`;
}

export function rosterRow(p,teams,prefs,recommended=false) {
  const locked=p.locked||p.game?.kickoff&&Date.parse(p.game.kickoff)<=Date.now();
  return `<article class="roster-row"><span class="slot">${esc(slot(recommended?p.assignedSlotId:p.lineupSlotId))}</span><div>${playerIdentity(p,teams)}<small>${esc(p.injuryStatus||'Status unknown')} · ${locked?'Locked / started':p.game?.kickoff?esc(date(p.game.kickoff)):'Kickoff unavailable'}</small></div><div class="score-strip">${metric('Actual',actualNumber(p.actualPoints))}${metric('ESPN proj.',number(p.projectedPoints))}${metric('Rank score',p.hasEstimate?number(p.decisionScore):'—')}</div>${detail('Player details',`${warnings(p.warnings)}<p>${esc(p.estimateSource)} · ${esc(p.matchupLabel)}</p>${preferenceButtons(p,prefs,true)}`)}</article>`;
}

export function waiverCard(p,teams,prefs) {
  return `<article class="card player team-${esc(p.team)}">${playerIdentity(p,teams)}<div class="pill">${esc(p.kind)} · ${p.mode==='ros'?'Rest of season':'This week'}</div><h3>${p.drop?`Drop ${esc(p.drop.name)}`:'Use your open roster slot'}</h3><div class="metric-grid">${metric('Lineup gain',signed(p.lineupGain))}${metric('Waiver edge',signed(p.waiverValue))}${metric(p.mode==='ros'?'Typical week':'Estimate',number(p.horizonEstimate))}</div>${detail('Why this move?',`${(p.reasons||[]).map(r=>`<p>${esc(r)}</p>`).join('')}<p>Season-long starter impact: ${signed(p.seasonCost)} estimated points per typical week.</p>${warnings(p.warnings)}<p>${esc(p.faabExplanation)}</p>${p.alternatives?.length?`<p>Other legal drops considered: ${p.alternatives.map(a=>`${esc(a.drop?.name||'open slot')} (${signed(a.lineupGain)} starter points)`).join('; ')}.</p>`:''}`)}${preferenceButtons(p,prefs)}</article>`;
}

export function tradeCard(t,teams={}) {
  return `<article class="card trade"><div class="eyebrow">${esc(t.otherTeam?.name||'BILATERAL REVIEW')}</div><h3>${t.viable?'Potential mutual fit':'Does not pass the fit screen'}</h3><div class="trade-sides"><div><span class="muted">You send</span>${(t.give||[]).map(p=>playerIdentity(p,teams)).join('')}</div><div><span class="muted">You receive</span>${(t.receive||[]).map(p=>playerIdentity(p,teams)).join('')}</div></div><p>${esc(t.reason)}</p>${t.yourImpact?`<div class="metric-grid">${metric('Your lineup gain',signed(t.yourImpact.gain))}${metric('Their lineup gain',signed(t.theirImpact.gain))}${metric('Value imbalance',`${number(t.imbalance)}%`)}</div>${detail('Both teams: before & after',`<p>Your typical-week starters: ${number(t.yourImpact.before)} → ${number(t.yourImpact.after)}; bench change ${signed(t.yourImpact.benchChange)}.</p><p>Their typical-week starters: ${number(t.theirImpact.before)} → ${number(t.theirImpact.after)}; bench change ${signed(t.theirImpact.benchChange)}.</p><p>Exchanged baseline: ${number(t.giveValue)} sent / ${number(t.receiveValue)} received.</p>${warnings(t.warnings)}`)}`:''}</article>`;
}

export function freshness(sources={}) {
  return detail('Data freshness & coverage',Object.entries(sources).map(([name,s])=>`<div class="source"><div><strong>${esc(name)}</strong><span class="status status-${esc(s.status)}">${esc(s.status)}</span></div><small>Updated ${esc(date(s.updatedAt))}${s.throughWeek!=null?` · Through week ${esc(s.throughWeek)}`:''}</small>${s.coverage?`<p>${esc(s.coverage)}</p>`:''}</div>`).join(''));
}
