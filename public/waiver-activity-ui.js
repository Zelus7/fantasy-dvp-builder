import {esc,date,warnings} from './ui.js';
const labels={won:'Won',unavailable:'Target unavailable',skipped:'Skipped',unsuccessful:'Unsuccessful',pending:'Pending',unresolved:'Unconfirmed'};
export function activityHtml(activity){
  if(!activity)return '<p>Automatic ESPN activity has not been checked.</p>';
  if(activity.error)return warnings([activity.error]);
  const pending=activity.pending,processed=activity.processed;
  return `<section class="claim-row"><div class="eyebrow">READ-ONLY ESPN SYNC</div><h3>Claims, results & next steps</h3><p>Last attempt ${esc(date(activity.checkedAt))}. Processed feed: <strong>${esc(processed.status)}</strong>. Pending feed: <strong>${esc(pending.status)}</strong>${pending.checkedAt?` · last readable ${esc(date(pending.checkedAt))}`:''}.</p>
    ${processed.status!=='fresh'?warnings(['Processed history is incomplete; safely classified results below are still confirmed. Ambiguous records are excluded from spend and bid guidance.']):''}
    <p class="fine">Processed coverage: ${(processed.weeks||[]).map(w=>`week ${w.week}: ${esc(w.status)}${w.ignored?` (${w.ignored} unclassified records)`:''}`).join(' · ')||'unavailable'}.</p>
    ${pending.status!=='fresh'?warnings(['Pending claims could not be freshly verified. An unavailable or stale feed does not mean there are no claims.']):''}
    ${activity.conflicts?warnings([`${activity.conflicts} conflicting ESPN record(s) are excluded from totals and bid guidance.`]):''}
    <p><strong>${activity.recentSpend===0&&processed.status!=='fresh'?'No confirmed spend total is available.':`$${activity.recentSpend} observed winning spend this scoring week.`}</strong> This is not a replacement for your fresh remaining FAAB balance.</p>
    <h4>Your pending claims</h4>${pending.rows.length?pending.rows.map(r=>`<p>${esc(r.playerName)} · $${r.bid} · drop ${esc(r.dropName||'none')}${pending.status==='stale'?' — last known, not currently confirmed':''}</p>`).join(''):`<p>${pending.status==='fresh'?'The explicit ESPN list returned no pending waiver claims for your team.':'No pending claims can be confirmed from this read.'}</p>`}
    ${(activity.claims||[]).length?`<h4>Saved-plan follow-up</h4>${activity.claims.map(c=>`<p><strong>${esc(c.addName)}: ${esc(labels[c.status]||c.status)}.</strong> ${esc(c.next)}</p>`).join('')}`:''}
    <details data-activity><summary>Your recorded ESPN outcomes (${activity.outcomes.length} latest)</summary>${activity.outcomes.map(r=>`<section class="claim-row"><h4>${esc(r.playerName)} — ${esc(labels[r.status])}</h4><p>Bid $${r.bid} · Paid $${r.paid} · Week ${r.week} · ${esc(date(r.at))}</p><p>${esc(r.reason)} Conditional drop: ${esc(r.dropName||'none')}.</p></section>`).join('')||'<p>No safely classified completed records have been archived yet.</p>'}</details><p class="fine">${esc(activity.explanation)}</p></section>`;
}

export function mountActivityDashboard(element,{api,query}){
  element.innerHTML='<article class="card"><h3>ESPN claim activity</h3><p>Checking pending claims and processed results…</p></article>';
  void api(`/api/claim-outcomes?${query()}`).then(data=>{
    if(element.isConnected)element.innerHTML=`<article class="card">${activityHtml(data.activity)}<button class="secondary" data-view="waivers">Review waiver options & audit trail</button></article>`;
  }).catch(()=>{if(element.isConnected)element.innerHTML=`<article class="card">${warnings(['Claim activity could not be loaded. Open Waivers to retry; no ESPN transaction was made.'])}</article>`;});
}
