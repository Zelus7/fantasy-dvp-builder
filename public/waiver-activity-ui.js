import {esc,date,warnings} from './ui.js';
const labels={won:'Won',unavailable:'Target unavailable',skipped:'Skipped',unsuccessful:'Unsuccessful',pending:'Pending',unresolved:'Unconfirmed'};
export function activityHtml(activity){
  if(!activity)return '<p>Automatic ESPN activity has not been checked.</p>';
  if(activity.error)return warnings([activity.error]);
  const pending=activity.pending,processed=activity.processed;
  const current=(activity.outcomes||[]).filter(r=>r.week===activity.currentWeek),older=(activity.outcomes||[]).filter(r=>r.week!==activity.currentWeek);
  const identity=r=>`${esc(r.playerName)}${r.nameMissing?` <small>(ESPN ID ${esc(r.playerId)})</small>`:''}`;
  const outcomes=rows=>rows.map(r=>`<section class="claim-row"><h4>${identity(r)} — ${esc(labels[r.status])}</h4><p>Bid $${r.bid} · Paid $${r.paid} · Week ${r.week} · ${esc(date(r.at))}</p><p>${esc(r.reason)} Conditional drop: ${esc(r.dropName||'none')}.</p></section>`).join('');
  const followUp=(activity.claims||[]).map(c=>`<p><strong>${esc(c.addName)}: ${esc(labels[c.status]||c.status)}.</strong> ${esc(c.next)}</p>`).join('');
  return `<section class="claim-row"><h3>Claims, results & next steps</h3><p>Read-only ESPN sync · Last attempt ${esc(date(activity.checkedAt))}</p>
    <h4>Your pending claims</h4><p class="fine">Pending feed: <strong>${esc(pending.status)}</strong>${pending.checkedAt?` · last readable ${esc(date(pending.checkedAt))}`:''}.</p>
    ${pending.status!=='fresh'?warnings(['Pending claims could not be freshly verified. An unavailable or stale feed does not mean there are no claims.']):''}
    ${pending.rows.length?pending.rows.map(r=>`<p><strong>${identity(r)}</strong> · $${r.bid} · drop ${esc(r.dropName||'none')}${pending.status==='stale'?' — last known, not currently confirmed':''}</p>`).join(''):`<p>${pending.status==='fresh'?'The explicit ESPN list returned no pending waiver claims for your team.':'No pending claims can be confirmed from this read.'}</p>`}
    <h4>Week ${esc(activity.currentWeek||'—')} confirmed results</h4>
    ${current.length?outcomes(current):'<p>No safely classified completed results for this week have been archived yet.</p>'}
    ${processed.status!=='fresh'?warnings(['Processed history is incomplete; safely classified results above are still confirmed. Ambiguous records are excluded from spend and bid guidance.']):''}
    <p><strong>${activity.recentSpend===0&&processed.status!=='fresh'?'No confirmed spend total is available.':`$${activity.recentSpend} observed winning spend this scoring week.`}</strong> This is not a replacement for your fresh remaining FAAB balance.</p>
    ${followUp?(activity.historicalPlan?`<details data-activity><summary>Archived worksheet follow-up · Week ${esc(activity.planWeek)}</summary><p>This is an older worksheet, not a recommendation to repeat those claims this week.</p>${followUp}</details>`:`<h4>Saved-plan follow-up</h4>${followUp}`):''}
    <details data-activity><summary>Earlier confirmed results (${older.length})</summary>${outcomes(older)||'<p>No earlier completed records have been archived yet.</p>'}</details>
    <details data-activity><summary>Sync coverage & limitations</summary><p>Processed feed: <strong>${esc(processed.status)}</strong>. ${(processed.weeks||[]).map(w=>`week ${w.week}: ${esc(w.status)}${w.ignored?` (${w.ignored} unclassified records)`:''}`).join(' · ')||'Coverage unavailable'}.</p>
    ${activity.conflicts?warnings([`${activity.conflicts} conflicting ESPN record(s) are excluded from totals and bid guidance.`]):''}
    ${activity.identityCoverage?.unresolved?`<p>${activity.identityCoverage.unresolved} player identities could not be verified. Names do not affect claim status or spend totals; refresh to retry.</p>`:''}
    <p class="fine">${esc(activity.explanation)}</p></details></section>`;
}

export function mountActivityDashboard(element,{api,query}){
  element.innerHTML='<article class="card"><h3>ESPN claim activity</h3><p>Checking pending claims and processed results…</p></article>';
  void api(`/api/claim-outcomes?${query()}`).then(data=>{
    if(element.isConnected)element.innerHTML=`<article class="card">${activityHtml(data.activity)}<button class="secondary" data-view="waivers">Review waiver options & audit trail</button></article>`;
  }).catch(()=>{if(element.isConnected)element.innerHTML=`<article class="card">${warnings(['Claim activity could not be loaded. Open Waivers to retry; no ESPN transaction was made.'])}</article>`;});
}
