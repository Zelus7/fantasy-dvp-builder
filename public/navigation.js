export const VIEWS = ['dashboard', 'lineup', 'matchups', 'compare', 'waivers', 'trades', 'settings'];
const MODES = ['week', 'bridge', 'ros'];
const POSITIONS = ['', 'QB', 'RB', 'WR', 'TE', 'K', 'D/ST'];

export function readRoute(hash = '') {
  const [view, query = ''] = hash.replace(/^#/, '').split('?');
  const p = new URLSearchParams(query);
  const week = Number(p.get('week'));
  return {
    current: VIEWS.includes(view) ? view : 'dashboard',
    mode: MODES.includes(p.get('mode')) ? p.get('mode') : 'week',
    tradeMode: MODES.includes(p.get('trade')) ? p.get('trade') : 'bridge',
    position: POSITIONS.includes(p.get('position')) ? p.get('position') : '',
    search: (p.get('q') || '').slice(0, 100),
    week: Number.isInteger(week) && week >= 1 && week <= 18 ? week : null,
    expanded: (p.get('open') || '').split(',').filter(id => /^[\w-]{1,100}$/.test(id)).slice(0, 24),
  };
}

export function writeRoute(state) {
  const p = new URLSearchParams();
  if (state.week != null) p.set('week', String(state.week));
  if (state.mode !== 'week') p.set('mode', state.mode);
  if (state.tradeMode !== 'bridge') p.set('trade', state.tradeMode);
  if (state.position) p.set('position', state.position);
  if (state.search) p.set('q', state.search);
  if (state.expanded?.length) p.set('open', state.expanded.slice(0, 24).join(','));
  return `#${VIEWS.includes(state.current) ? state.current : 'dashboard'}${p.size ? `?${p}` : ''}`;
}

export function filterRecommendations(players = [], query = '') {
  const term = query.trim().toLocaleLowerCase();
  return players.filter(p => !term || [p.name, p.drop?.name, p.team, p.position].filter(Boolean).join(' ').toLocaleLowerCase().includes(term));
}
