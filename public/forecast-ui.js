import {esc,number,signed,date,detail,metric,warnings} from './ui.js';
import {intelligenceEvidence} from './intelligence-ui.js';

export function forecastEvidence(p){
  if(!['QB','RB','WR','TE'].includes(p.position))return '';
  const f=p.forecast;
  const context=intelligenceEvidence(p);
  if(!f)return context+'<p class="fine">Independent forecast unavailable: history, matching week or fresh inputs are missing. This is not a zero-point prediction.</p>';
  const weekly=f.horizons['1'],four=f.horizons['4'];
  return context+detail('Independent forecast & expected workload',`
    <p><strong>Independent statistical forecast—not ESPN.</strong> ${f.usable?'These estimates are conditional on playing.':'Context warning: do not use this forecast for a lineup move.'}</p>
    <div class="metric-grid">${metric(`W${f.targetWeek} points if playing`,number(weekly.points))}${metric('Expected targets',number(weekly.targets))}${metric('Expected carries',number(weekly.carries))}</div>
    <p>ESPN this week: ${number(p.projectedPoints)}. ${weekly.approvedFallback?'Qualified against the historical fallback, not against ESPN.':'Research only: the one-week model did not pass the historical promotion gate.'} ESPN remains the primary projection when available.</p>
    <p>Historical 80% interval: ${number(weekly.low)} to ${number(weekly.high)} points. Measured 2025 coverage: ${number(weekly.measuredCoverage*100)}% across ${esc(weekly.intervalTestCount)} ${esc(p.position)} observations. This is population-level evidence, not a guaranteed 80% chance for this particular player.</p>
    ${four?`<p><strong>Next four calendar weeks:</strong> ${number(four.points)} points per recorded appearance; ${number(four.targets)} targets and ${number(four.carries)} carries per appearance. Not a four-week total, and missed games/byes are not assumed to score this amount.</p><p>Four-week historical interval: ${number(four.low)}–${number(four.high)} points per appearance; measured coverage ${number(four.measuredCoverage*100)}%. ${four.approvedFallback?'Passed the historical fallback gate. It can replace a purely historical baseline within this window when ESPN projections are absent.':'Research only; retained baseline performed better on at least one required check.'}</p>`:'<p>Four-week forecast unavailable beyond week 15; do not extrapolate past the evaluated season window.</p>'}
    ${warnings(f.warnings)}
    <p class="fine">${esc(f.version)} · input games strictly before W${f.targetWeek}, ${f.season}. Learned from 2019–2023; intervals calibrated on 2024 and evaluated on 2025. Points and usage are learned separately from past usage, share, efficiency and production. Current news, depth and red-zone evidence remain context, not trained inputs. Injury recovery and unexpected role changes are not predicted.</p>`);
}

export function forecastResearch(players=[],position=''){
  const options=players.filter(p=>(!position||p.position===position)&&p.forecast?.usable&&p.isAvailable!==false&&
    ['FREEAGENT','WAIVERS'].includes(p.status)&&!Number(p.onTeamId||0)&&
    !(p.game?.kickoff&&Date.parse(p.game.kickoff)<=Date.now()));
  const score=p=>p.forecast.horizons['4']?.points??p.forecast.horizons['1'].points;
  const groups=['QB','RB','WR','TE'].map(pos=>({position:pos,players:options.filter(p=>p.position===pos)
    .sort((a,b)=>score(b)-score(a)||String(a.playerId).localeCompare(String(b.playerId))).slice(0,position?8:2)})).filter(g=>g.players.length);
  return detail('Forecast lab: candidates beyond this week',`
    <p>This is a research view of the loaded available-player pool: up to two candidates per position, or eight for the selected position. Candidates are sorted within each position by independent four-week points per appearance (one-week estimates when the four-week window is unavailable). Raw points across different positions are not a measure of waiver value. It is <strong>not an add/drop ranking</strong>: it does not price the player you would drop, injury risk, positional need or your budget. Use the legal roster comparisons above before making a move.</p>
    <p>Four-week WR and TE models passed the historical fallback checks. No one-week model earned replacement of the baseline. Passing that test does not establish an edge over ESPN. Historical lower-ranked pools are proxies, not your league's actual past waivers.</p>
    ${groups.length?groups.map(g=>`<section><h3>${esc(g.position)} research candidates</h3>${g.players.map(p=>`<article class="card"><h4>${esc(p.name)} · ${esc(p.position)}</h4>${forecastEvidence(p)}</article>`).join('')}</section>`).join(''):'<p>No eligible, fresh independent forecasts are loaded for this position. Only QB, RB, WR and TE are modeled; unfamiliar players may have insufficient history.</p>'}`);
}

export function forecastAudit(a){
  const c=a?.coverage;
  const coverage=c?`<p>Audit coverage: ${c.uniquePlayers} unique players; ${c.matched} matched. Exclusions (each player counted once): ${c.noUsableForecast} without a usable independent forecast, ${c.notPreKickoff} not demonstrably frozen before kickoff, ${c.missingActual} missing actuals, ${c.missingEspn} missing ESPN projections. Neither missing nor excluded means zero points.</p>`:'';
  if(!a?.count)return '<p>Independent forecast comparison awaits matched pre-kickoff predictions and final scores.</p>'+coverage;
  return `<p><strong>Independent forecast vs ESPN:</strong> ${a.count} matched players. Mean absolute error ${number(a.modelMae)} vs ${number(a.espnMae)} points (lower is better). ${esc(a.caveat)} Repeated snapshots are not independent samples.</p>${coverage}${detail('Player-level prediction errors',`<p>Largest absolute independent-model errors first. Signed error = prediction minus actual: positive means overprediction. Same players and decision time for both models.</p><p class="fine">On a phone, swipe the table sideways to see every score and error.</p><div class="table-scroll" tabindex="0" aria-label="Player prediction errors, horizontally scrollable"><table class="forecast-error-table"><thead><tr><th>Player</th><th>Independent</th><th>ESPN</th><th>Actual</th><th>Model error</th><th>ESPN error</th></tr></thead><tbody>${(a.rows||[]).map(r=>`<tr><td>${esc(r.name)} · ${esc(r.position)}</td><td>${number(r.prediction)}</td><td>${number(r.espn)}</td><td>${number(r.actual)}</td><td>${signed(r.error)}</td><td>${signed(r.espnError)}</td></tr>`).join('')}</tbody></table></div>${a.missing?.length?`<h4>Unmatched players</h4><ul>${a.missing.map(p=>`<li>${esc(p.name)}: ${esc(p.reason)}</li>`).join('')}</ul>`:''}`)}`;
}

export function decisionAuditPanel(a,coverage){
  if(!a)return '<p>Detailed lineup audit unavailable for this archived calculation.</p>';
  const m=a.metrics;
  return `<h4>Separate decision results</h4><p>Inputs frozen ${esc(date(a.timing.frozenAt))}; first loaded kickoff ${esc(date(a.timing.firstKickoff))}. ${a.timing.prospective?'Clean pre-week snapshot.':'Not a clean pre-week snapshot; individual forecasts still require a pre-kickoff timestamp.'}</p>
    <div class="metric-grid">${metric('Start / sit gain',signed(m.startSitGain))}${metric('Waiver net starter gain',signed(m.waiverNetGain))}${metric('Points / FAAB dollar',number(m.faabPointsPerDollar))}</div><p>${esc(a.explanation)}</p><p>${esc(m.acquisitionStatus)}. Unknown metrics stay blank; a free acquisition does not create infinite efficiency.</p>
    ${detail('Exact frozen lineups and missing scores',Object.entries(a.lineups).filter(([,v])=>v).map(([key,l])=>`<h4>${esc(({kept:'Keep roster — suggested starters',proposed:'Proposed add/drop starters',espn:'ESPN-only baseline starters',asSet:'Lineup as set at freeze time'})[key])}</h4><p>${l.matched}/${l.expected} actuals matched; total ${number(l.points)}.${l.missing.length?` Missing: ${l.missing.map(p=>esc(p.name)).join(', ')}.`:''}</p><div class="table-scroll"><table><thead><tr><th>Player</th><th>Frozen estimate</th><th>Actual</th></tr></thead><tbody>${l.rows.map(p=>`<tr><td>${esc(p.name)}</td><td>${number(p.estimate)}</td><td>${number(p.actual)}</td></tr>`).join('')}</tbody></table></div>`).join(''))}
    ${coverage?`<p>Historical-score retrieval: ${coverage.matched}/${coverage.expected} players matched. ${esc(coverage.scope)}</p>${coverage.missing.length?detail(`Missing historical actuals (${coverage.missing.length})`,`<ul>${coverage.missing.map(p=>`<li>${esc(p.name)} · ${esc(p.position)}</li>`).join('')}</ul>`):''}`:''}
    ${detail('Did the observed role persist?',m.rolePersistence.length?`<p>Compares the elevated usage known at decision time with the next exact game-week observation. It is not a fantasy-point forecast, proof of acquisition, or a guarantee of future workload. Missing games remain unknown.</p><ul>${m.rolePersistence.map(r=>`<li>${esc(r.name)} · ${esc(r.metric)}: W${r.beforeWeek} ${number(r.before)} → W${r.afterWeek} ${number(r.after)}${r.retainedRatio!=null?` (${number(r.retainedRatio*100)}% retained)`:' (observation unavailable)'}. ${r.metric==='snapShare'?'Shares are fractions of 1.':''}</li>`).join('')}</ul>`:'<p>No dated numeric role-change signal was captured by this model version. Historical points alone cannot reconstruct a workload prediction.</p>')}`;
}
