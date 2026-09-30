import {esc,number,date,detail,warnings} from './ui.js';
import {playerIntelligence} from './model/intelligence.js';

const link=row=>row?.sourceUrl&&/^https:\/\/(www\.nfl\.com|www\.espn\.com|sumersports\.com)\//.test(row.sourceUrl)
  ? `<a href="${esc(row.sourceUrl)}" target="_blank" rel="noopener noreferrer">${esc(row.source||'Source')}</a>`:'';
export function intelligenceEvidence(p){
  if(!['QB','RB','WR','TE'].includes(p.position))return '';
  const o=p.feature?.opportunity,e=playerIntelligence(p);
  const weekly=(o?.weeklyUsage||[]).slice(-5);
  return detail('Usage, routes & availability evidence',`
    <p>Observed context, not extra projected points. Missing values are unknown, not zero.</p>
    ${e.items.map(t=>`<p>${esc(t)}</p>`).join('')}${warnings(e.warnings)}
    ${e.routes?`<p>${link(e.routes)} · source updated ${esc(date(e.routes.sourceUpdatedAt))} · fetched ${esc(date(e.routes.fetchedAt))}.</p><p class="fine">Season-to-date WR routes only. Weekly route counts, route participation and RB/TE route coverage are not supplied by this public feed. Targets per route and yards per route are different measures.</p>`:''}
    ${weekly.length?`<h4>Recent observed usage</h4>${weekly.map(w=>`<p><strong>W${esc(w.week)}</strong>: ${number(w.targets)} targets · ${number(w.carries)} carries · ${w.snapShare==null?'—':`${number(w.snapShare*100)}%`} offensive snaps · ${number(w.receivingAirYards)} receiving air yards.</p>`).join('')}<p class="fine">Air yards measure target distance, not yards gained. Snap share includes blocking/running plays and is not route participation. Only recorded appearances are shown.</p>`:'<p>Current-season game-by-game usage is unavailable.</p>'}
    ${e.practice?`<p>${link(e.practice)} · observed ${esc(date(e.practice.fetchedAt))}. The page does not identify the practice day; this is not a Wednesday/Thursday/Friday trend.</p>`:'<p>No matched official practice report is loaded. That does not establish that the player practiced fully or will play.</p>'}
    ${e.injuries?`<p>${esc(e.injuries.headline)}</p><p>${link(e.injuries)} · individual report ${esc(date(e.injuries.reportedAt))} · feed checked ${esc(date(e.injuries.fetchedAt))}.</p>`:''}
    <p class="fine">ESPN fantasy availability remains authoritative for the current roster. Reports do not supply guaranteed recovery dates, confirmed inactive coverage or a complete discipline/role-change feed. These new inputs inform evidence and cautions; forecast coefficients are unchanged until separately tested.</p>`);
}
