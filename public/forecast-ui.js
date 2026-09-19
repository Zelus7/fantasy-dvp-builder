import {esc,number,detail,metric,warnings} from './ui.js';

export function forecastEvidence(p){
  if(!['QB','RB','WR','TE'].includes(p.position))return '';
  const f=p.forecast;
  if(!f)return '<p class="fine">Independent forecast unavailable: history, matching week or fresh inputs are missing. This is not a zero-point prediction.</p>';
  const weekly=f.horizons['1'],four=f.horizons['4'];
  return detail('Independent forecast & expected workload',`
    <p><strong>Independent statistical forecast—not ESPN.</strong> ${f.usable?'These estimates are conditional on playing.':'Context warning: do not use this forecast for a lineup move.'}</p>
    <div class="metric-grid">${metric('Next game forecast',number(weekly.points))}${metric('Expected targets',number(weekly.targets))}${metric('Expected carries',number(weekly.carries))}</div>
    <p>ESPN this week: ${number(p.projectedPoints)}. ${weekly.approvedFallback?'Qualified against the historical fallback, not against ESPN.':'Research only: the one-week model did not pass the historical promotion gate.'} ESPN remains the primary projection when available.</p>
    <p>Historical 80% interval: ${number(weekly.low)} to ${number(weekly.high)} points. Measured 2025 coverage: ${number(weekly.measuredCoverage*100)}% across ${esc(weekly.intervalTestCount)} ${esc(p.position)} observations. This is population-level evidence, not a guaranteed 80% chance for this particular player.</p>
    ${four?`<p><strong>Next four calendar weeks:</strong> ${number(four.points)} points per recorded appearance; ${number(four.targets)} targets and ${number(four.carries)} carries per appearance. Not a four-week total, and missed games/byes are not assumed to score this amount.</p><p>Four-week historical interval: ${number(four.low)}–${number(four.high)} points per appearance; measured coverage ${number(four.measuredCoverage*100)}%. ${four.approvedFallback?'Passed the historical fallback gate. It can replace a purely historical baseline within this window when ESPN projections are absent.':'Research only; retained baseline performed better on at least one required check.'}</p>`:'<p>Four-week forecast unavailable beyond week 15; do not extrapolate past the evaluated season window.</p>'}
    ${warnings(f.warnings)}
    <p class="fine">${esc(f.version)} · input games strictly before W${f.targetWeek}, ${f.season}. Learned from 2019–2023; intervals calibrated on 2024 and evaluated on 2025. Points and usage are learned separately from past usage, share, efficiency and production. Current news, depth and red-zone evidence remain context, not trained inputs. Injury recovery and unexpected role changes are not predicted.</p>`);
}

export function forecastResearch(players=[]){
  const options=players.filter(p=>p.forecast?.usable&&p.isAvailable!==false&&
    ['FREEAGENT','WAIVERS'].includes(p.status)&&!Number(p.onTeamId||0)&&
    !(p.game?.kickoff&&Date.parse(p.game.kickoff)<=Date.now()));
  const ranked=[...options].sort((a,b)=>(b.forecast.horizons['4']?.points??b.forecast.horizons['1'].points)-(a.forecast.horizons['4']?.points??a.forecast.horizons['1'].points)).slice(0,8);
  return detail('Forecast lab: candidates beyond this week',`
    <p>This is a research view of the loaded available-player pool, sorted by independent four-week points per appearance. It is <strong>not an add/drop ranking</strong>: it does not price the player you would drop, injury risk, positional need or your budget. Use the legal roster comparisons above before making a move.</p>
    <p>Four-week WR and TE models passed the historical fallback checks. No one-week model earned replacement of the baseline. Passing that test does not establish an edge over ESPN. Historical lower-ranked pools are proxies, not your league's actual past waivers.</p>
    ${ranked.length?ranked.map(p=>`<article class="card"><h3>${esc(p.name)} · ${esc(p.position)}</h3>${forecastEvidence(p)}</article>`).join(''):'<p>No eligible, fresh independent forecasts are loaded. Refresh; unfamiliar players may have insufficient history.</p>'}`);
}

export function forecastAudit(a){
  if(!a?.count)return '<p>Independent forecast comparison awaits matched pre-kickoff predictions and final scores.</p>';
  return `<p><strong>Independent forecast vs ESPN:</strong> ${a.count} matched players. Mean absolute error ${number(a.modelMae)} vs ${number(a.espnMae)} points (lower is better). ${esc(a.caveat)} Each snapshot is a separate trial; repeated snapshots must not be added as independent samples.</p>`;
}
