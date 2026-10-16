import { candidates, readState, stateHash, matchingCandidates, candidateSummary } from './preview-data.js';
const $ = selector => document.querySelector(selector);
const dialog = $('#preview-dialog');
let lastView = '';
const state = () => readState(location.hash);
function navigate(changes, replace = false) {
  const hash = stateHash({ ...state(), ...changes });
  if (replace) history.replaceState(null, '', hash);
  else location.hash = hash;
}
function showMessage(title, message) {
  $('#dialog-title').textContent = title;
  $('#dialog-message').textContent = message;
  dialog.showModal();
}
function candidateCard(p, s) {
  const brief = candidateSummary(p, s.period), weekly = s.period === 'week';
  return `<details class="target" data-candidate="${p.id}" ${s.open.includes(p.id) ? 'open' : ''}>
    <summary><span class="team-mark ${p.mark}" aria-hidden="true">${p.team}</span>
      <span class="identity"><strong>${p.name}</strong><span>${p.position} · ${p.team}</span></span>
      <span class="gain"><strong>${brief.gain}${weekly ? ' <small>pts</small>' : ''}</strong><span>${brief.condition}</span></span>
      <span class="chevron" aria-hidden="true">⌄</span>
      <span class="summary-headline">${brief.headline}</span>
      <span class="summary-move">${weekly ? `For ${p.replacement} · Drop ${p.drop}` : `Possible drop: ${p.drop} · Not approved`}</span>
    </summary>
    <div class="target-body">
      <h3>${weekly ? p.verdict : 'Hold until the future cost is checked'}</h3>
      ${weekly ? `<table class="comparison"><caption>${p.comparison} · Sample estimates</caption><thead><tr><th scope="col">Player</th><th scope="col">Projected points</th></tr></thead><tbody><tr><th scope="row">${p.name}<small>Available ${p.position}</small></th><td>${p.estimate}</td></tr><tr><th scope="row">${p.replacement}<small>Your current fallback / starter</small></th><td>${p.baseline}</td></tr></tbody><tfoot><tr><th scope="row">Lineup change</th><td>${p.gain}</td></tr></tfoot></table>${p.id === 'reed' ? '<p class="comparison-note">If Foster plays: <strong>0.0-point gain</strong>. Keep your normal lineup. The injury-only gain is not added to it.</p>' : ''}` : '<p class="comparison-note">No future point total or starting-week count is established. Weekly gains are deliberately hidden in this view.</p>'}
      <p class="action-rule">${weekly ? p.action : p.future}</p>
      <section><h4>Who you give up</h4><p>${p.cost}</p></section>
      <section><h4>${weekly ? 'When to decide' : 'Next check'}</h4><p>${weekly ? p.deadline : p.followup}</p></section>
      <section><h4>What supports the idea</h4><dl class="evidence">${p.evidence.map(([label, value, scope]) => `<div><dt>${label}<small>${scope}</small></dt><dd>${value}</dd></div>`).join('')}</dl><p class="sample-caption">Fictional evidence to demonstrate the layout, not live player data.</p></section>
      <section><h4>What could change the call</h4><p>${p.missing}</p><p class="decision-rule">${p.change}</p></section>
      ${weekly ? `<section><h4>After this week</h4><p>${p.followup}</p></section>` : ''}
      <button class="quiet" data-draft="${p.id}">Review sample add / drop</button>
    </div>
  </details>`;
}
function render() {
  const s = state(), rows = matchingCandidates(s);
  for (const view of ['dashboard', 'waivers']) $('#' + view).hidden = view !== s.view;
  for (const link of document.querySelectorAll('[data-nav]')) {
    if (link.dataset.nav === s.view) link.setAttribute('aria-current', 'page');
    else link.removeAttribute('aria-current');
  }
  for (const button of document.querySelectorAll('[data-period]')) button.setAttribute('aria-pressed', String(button.dataset.period === s.period));
  $('#position').value = s.position;
  $('#search').value = s.query;
  $('#filters').open = s.filters;
  $('#filter-status').textContent = s.position !== 'All' || s.query ? 'Filters applied' : 'Filter players';
  $('#result-count').textContent = `${rows.length} sample ${s.period === 'week' ? 'targets' : 'research candidates'}`;
  $('#horizon-note').textContent = s.period === 'week' ? 'Tap a player for the comparison, drop cost & decision rules.' : 'Future gains are unverified. These are research candidates, not recommended adds.';
  $('#empty').hidden = rows.length > 0;
  $('#targets').innerHTML = rows.map(p => candidateCard(p, s)).join('');
  if (s.view !== lastView) { lastView = s.view; window.scrollTo({ top: 0, behavior: 'instant' }); }
  $('#announcement').textContent = s.view === 'waivers' ? `${rows.length} sample targets. ${s.period === 'week' ? 'This week.' : 'Future gains unverified.'}` : 'Dashboard preview.';
  $('#claims').open = s.section === 'claims';
  if (s.section === 'claims') $('#claims').scrollIntoView({ block: 'start' });
}
document.addEventListener('click', event => {
  const period = event.target.closest('[data-period]'), draft = event.target.closest('[data-draft]');
  if (period) navigate({ period: period.dataset.period, section: null });
  if (draft) {
    const p = candidates.find(p => p.id === draft.dataset.draft);
    showMessage(`Sample: ${p.name} for ${p.drop}`, `${state().period === 'week' ? p.action : p.future} This preview cannot save or submit anything. No FAAB bid is set.`);
  }
});
$('#targets').addEventListener('toggle', event => {
  if (!event.target.matches('[data-candidate]')) return;
  navigate({ open: [...document.querySelectorAll('[data-candidate][open]')].map(el => el.dataset.candidate) }, true);
}, true);
$('#filters').addEventListener('toggle', () => navigate({ filters: $('#filters').open }, true));
$('#claims').addEventListener('toggle', () => navigate({ section: $('#claims').open ? 'claims' : null }, true));
$('#search').addEventListener('input', event => { navigate({ query: event.target.value, section: null }, true); render(); });
$('#position').addEventListener('change', event => { navigate({ position: event.target.value, section: null }, true); render(); });
$('#clear-filters').addEventListener('click', () => { navigate({ position: 'All', query: '', section: null }, true); render(); });
$('.skip').addEventListener('click', event => { event.preventDefault(); $('#main').focus(); $('#main').scrollIntoView({ block: 'start' }); });
$('#about').addEventListener('click', () => showMessage('Fictional design preview', 'All names, stats, deadlines and decisions are fictional examples. No live data is read and nothing is sent to ESPN. The production layout will only be replaced after your approval.'));
$('#close-dialog').addEventListener('click', () => dialog.close());
$('#dialog-done').addEventListener('click', () => dialog.close());
window.addEventListener('hashchange', render);
render();
