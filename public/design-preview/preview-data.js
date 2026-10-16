// Fictional fixtures only. No league reads or real recommendations.
export const candidates = [
  {
    id: 'reed', name: 'Miles Reed', team: 'TEN', position: 'RB', mark: 'ten',
    gain: '+2.3', condition: 'if Foster is out', normal: '0.0',
    headline: 'Injury cover. Keep your starters if Foster plays.',
    replacement: 'Evan Cole', drop: 'Noah Brooks', estimate: '11.2', baseline: '8.9',
    comparison: 'Starting RB if Avery Foster is out', verdict: 'Wait for the injury update',
    action: 'If Foster is ruled out before Reed locks, add Reed and start him over Cole. If Foster is cleared, keep your roster: this move adds 0.0 points to your normal lineup.',
    cost: 'Drop Noah Brooks, your bench WR (6.2-point estimate). He is not in your best Week 6 or Week 7 lineup in this example, so the modeled starting-lineup cost is 0.0 points in those weeks. You still lose WR injury cover; Week 8 and beyond are not checked.',
    deadline: 'Decide by Sunday 12:45 PM ET, before Reed’s 1:00 PM game. Foster plays at 4:25 PM, so his final inactive announcement may come too late. If he is still uncertain, Reed is insurance only; keep Cole if you do not want to release Brooks for that risk.',
    evidence: [ ['Targets / game', '4.7', 'Last 3 games'], ['Offensive snaps', '54%', 'Last 3 games'], ['Routes run', 'Missing', 'Cannot verify receiving role'] ],
    missing: 'Foster’s latest practice report is missing. That decides whether you need the cover at all. Reed’s routes are also missing, so targets alone do not justify paying for a sustained receiving role.',
    change: 'Foster cleared → hold Brooks. Foster ruled out before Reed locks → use Reed. Reed’s role shrinks → keep Cole. Do not place a paid bid from this evidence alone.',
    followup: 'Monday: if Foster is returning, compare Reed with your next bye-week need. Keep him only if he still has a starting window; this is not an automatic season-long stash.',
    future: 'No verified multiweek starting window. Check Foster’s return and Brooks’s future WR-cover value before releasing him.'
  },
  {
    id: 'ellis', name: 'Jordan Ellis', team: 'PHI', position: 'WR', mark: 'phi',
    gain: '+1.4', condition: 'normal lineup', normal: '+1.4',
    headline: 'Small gain; watch the role before spending.',
    replacement: 'Luca Grant', drop: 'Owen Hayes', estimate: '9.4', baseline: '8.0',
    comparison: 'Starting WR with the normal lineup', verdict: 'Keep Grant for now',
    action: 'Ellis projects 1.4 points above Grant, but the workload jump is only one game old and practice clearance is missing. Keep Grant and Hayes until those checks support the move; no paid claim yet.',
    cost: 'Drop Owen Hayes, your bench WR (6.5-point estimate), not starter Luca Grant. Hayes is not in the best Week 6 lineup, but you would lose a possible Week 7 bye replacement. That future loss has not been quantified, so the 1.4-point gain is not enough to approve the drop.',
    deadline: 'Recheck the final practice report before Sunday’s games. If clearance or the receiving role is still unresolved, keep Grant. Do not interpret a positive estimate as a must-add.',
    evidence: [ ['Targets', '3 → 7', 'Previous game → latest'], ['Offensive snaps', '42% → 68%', 'Previous game → latest'], ['Routes run', 'Missing', 'One-week role change unconfirmed'] ],
    missing: 'We need practice clearance, route participation, and the reason for the extra work. A teammate’s temporary absence could explain the jump. One game cannot establish the role will last.',
    change: 'Cleared + sustained receiving role + Hayes’s bye cover replaced → review the add. Teammate returns or routes fall → hold. The app must show the new evidence before changing this call.',
    followup: 'After the next game, compare routes and targets with this week. Reassess the bench spot before the Week 7 bye; do not carry Ellis just because you added him.',
    future: 'Role durability and Hayes’s Week 7 replacement cost are unverified. Keep Hayes until both are checked.'
  }
];
const positions = ['All', 'QB', 'RB', 'WR', 'TE', 'K', 'D/ST'];
export function readState(hash = '') {
  const [view, query] = hash.replace(/^#/, '').split('?');
  const params = new URLSearchParams(query);
  return {
    view: view === 'waivers' ? 'waivers' : 'dashboard',
    period: ['week', 'bridge', 'ros'].includes(params.get('period')) ? params.get('period') : 'week',
    position: positions.includes(params.get('position')) ? params.get('position') : 'All',
    query: (params.get('q') || '').slice(0, 100),
    open: [...new Set((params.get('open') || '').split(',').filter(id => candidates.some(p => p.id === id)))],
    filters: params.get('filters') === '1', section: params.get('section') === 'claims' ? 'claims' : null
  };
}
export function stateHash(state) {
  const p = new URLSearchParams();
  if (state.period !== 'week') p.set('period', state.period);
  if (state.position !== 'All') p.set('position', state.position);
  if (state.query) p.set('q', state.query);
  if (state.open.length) p.set('open', state.open.join(','));
  if (state.filters) p.set('filters', '1');
  if (state.section) p.set('section', state.section);
  return '#' + state.view + (p.size ? '?' + p : '');
}
export function matchingCandidates(state) {
  return candidates.filter(p => (state.position === 'All' || p.position === state.position)
    && `${p.name} ${p.team}`.toLowerCase().includes(state.query.toLowerCase()));
}
export function candidateSummary(player, period) {
  return period === 'week'
    ? { gain: player.gain, condition: player.condition, headline: player.headline }
    : { gain: 'Not ranked', condition: period === 'bridge' ? 'Next 4 weeks' : 'Rest of season', headline: player.future };
}
