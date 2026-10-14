// Deliberately isolated design prototype: no fetch, credentials, persistence, or transactions.
const samples=[
  {name:'Miles Reed',team:'TEN',position:'RB',mark:'ten',type:'Injury cover',tag:'amber',normal:'0.0',conditional:'+2.3',why:'If Avery Foster is out, Reed improves your best fallback lineup by 2.3 points. If Foster plays, keep your normal starters.',role:'Sample role evidence: 4.8 targets per game over three games. Route coverage is incomplete; targets are not a substitute for routes.',cost:'Use an open roster slot in this example. No existing player is dropped. A real recommendation must name and price the actual drop.',next:'Check the practice report. Reed plays earlier than Foster, so decide before Sunday at 1:00 PM. If Foster is cleared, reassess this insurance spot next week.'},
  {name:'Jordan Ellis',team:'PHI',position:'WR',mark:'phi',type:'Watch the role',tag:'teal',normal:'+1.4',conditional:'—',why:'A small projected lineup improvement, but the recent workload increase is only one game old. Worth watching; not a confident paid claim.',role:'Sample role evidence: targets increased from 3 to 7 in the latest game. One game does not establish a trend. Current practice clearance is missing.',cost:'The example comparison replaces an 8.0-point receiver with a 9.4-point estimate. Before acting, inspect the named drop and next-week schedule.',next:'Look for a second week of sustained routes and targets. If the role falls back, keep your existing receiver.'}
];
const $=selector=>document.querySelector(selector),dialog=$('#preview-dialog');
let lastView='';
function showMessage(message){$('#dialog-message').textContent=message;dialog.showModal();}
function state(){const [view,query]=(location.hash.slice(1)||'dashboard').split('?');const params=new URLSearchParams(query);return {view:view==='main'?lastView||'dashboard':view==='waivers'?'waivers':'dashboard',period:['week','bridge','ros'].includes(params.get('period'))?params.get('period'):'week',position:params.get('position')||'All',query:params.get('q')||'',section:params.get('section')}}
function navigate(changes){const next={...state(),...changes},params=new URLSearchParams();for(const key of ['period','position'])if(next[key]!==({period:'week',position:'All'})[key])params.set(key,next[key]);if(next.query)params.set('q',next.query);location.hash=next.view+(params.size?'?'+params:'');}
function render(){
  const s=state();
  for(const view of ['dashboard','waivers'])$('#'+view).hidden=view!==s.view;
  for(const link of document.querySelectorAll('[data-nav]')){if(link.dataset.nav===s.view)link.setAttribute('aria-current','page');else link.removeAttribute('aria-current');}
  for(const button of document.querySelectorAll('[data-period]'))button.setAttribute('aria-pressed',String(button.dataset.period===s.period));
  for(const button of document.querySelectorAll('[data-position]'))button.setAttribute('aria-pressed',String(button.dataset.position===s.position));
  $('#search').value=s.query;
  $('#horizon-note').textContent=s.period==='week'?'Start with holding your roster. Separate normal-lineup gains from injury-only coverage.':s.period==='bridge'?'Four-week view: verify the exact starting weeks, the drop cost, and what happens when injured teammates return. Sample research only.':'Season view: use availability bookends and bye coverage, not one exact future point total. These sample targets need additional evidence.';
  const rows=samples.filter(p=>(s.position==='All'||p.position===s.position)&&`${p.name} ${p.team}`.toLowerCase().includes(s.query.toLowerCase()));
  $('#result-count').textContent=`${rows.length} sample ${rows.length===1?'target':'targets'} · Not live recommendations`;
  $('#empty').hidden=rows.length>0;
  $('#targets').innerHTML=rows.map(p=>`<article class="target"><div class="player-heading"><span class="team-mark ${p.mark}" aria-hidden="true">${p.team}</span><div><h3>${p.name}</h3><p>${p.position} · ${p.team} · Sample free agent</p></div><span class="tag ${p.tag}">${s.period==='week'?p.type:'Research only'}</span></div><p class="target-why">${s.period==='week'?p.why:'No verified future starting window is established for this sample. Review role durability and your returning starters before using a roster spot.'}</p><dl class="target-metrics"><div><dt>${s.period==='week'?'Normal-lineup gain':'Future starting weeks'}</dt><dd>${s.period==='week'?p.normal:'Unverified'} ${s.period==='week'?'<small>points</small>':''}</dd></div><div><dt>${s.period==='week'?'If Foster is out':'Season forecast'}</dt><dd>${s.period==='week'?p.conditional:'Not established'} ${s.period==='week'&&p.conditional!=='—'?'<small>points</small>':''}</dd></div></dl><details><summary>Evidence, drop cost & next step</summary><p><strong>Role:</strong> ${p.role}</p><p><strong>Roster cost:</strong> ${p.cost}</p><p><strong>What comes next:</strong> ${p.next}</p><p><strong>Uncertainty:</strong> These illustrative estimates are not confidence intervals or win probabilities. Confirm active status and acquisition timing.</p></details><div class="target-actions"><button class="quiet" data-draft="${p.name}">Preview manual review</button><p>No automated paid bid. Nothing is submitted.</p></div></article>`).join('');
  if(s.view!==lastView){lastView=s.view;window.scrollTo({top:0,behavior:'instant'});$('#announcement').textContent=s.view==='waivers'?'Waiver design preview opened.':'Dashboard design preview opened.';}
  if(s.section==='claims')$('#claims').scrollIntoView({block:'start'});
}
document.addEventListener('click',event=>{
  const period=event.target.closest('[data-period]'),position=event.target.closest('[data-position]'),draft=event.target.closest('[data-draft]');
  if(period)navigate({period:period.dataset.period});if(position)navigate({position:position.dataset.position});
  if(draft)showMessage(`In the finished design, this would open a review of ${draft.dataset.draft}, the named drop, and your spending limit. This prototype does not save a plan or send anything to ESPN.`);
});
$('#search').addEventListener('input',event=>{const s=state(),params=new URLSearchParams();if(s.period!=='week')params.set('period',s.period);if(s.position!=='All')params.set('position',s.position);if(event.target.value)params.set('q',event.target.value);history.replaceState(null,'','#waivers'+(params.size?'?'+params:''));render();});
$('#clear-filters').addEventListener('click',()=>navigate({position:'All',query:''}));
$('#about').addEventListener('click',()=>showMessage('This is a mobile-first Dashboard and Waivers design proposal. Every player and number is illustrative. Try both screens, the horizon and position filters, and the evidence disclosures. The production layout will only be replaced after your approval.'));
$('#close-dialog').addEventListener('click',()=>dialog.close());$('#dialog-done').addEventListener('click',()=>dialog.close());
window.addEventListener('hashchange',render);render();
