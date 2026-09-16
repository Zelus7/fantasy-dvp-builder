// Curated source evidence is separate from speculative planning scenarios.
// Never extend reviewBy automatically just because the app was refreshed.
const INITIAL=[{playerId:'4047646',season:2026,name:'A.J. Brown',earliestWeek:6,earlyWeek:6,planningWeek:7,lateWeek:9,
  checkedAt:'2026-09-15T23:30:00Z',reviewBy:'2026-09-18T23:30:00Z',
  sourceUrl:'https://www.patriots.com/news/analysis-patriots-place-wr-a-j-brown-on-injured-reserve-sign-dt-daquan-jones',
  note:'Official earliest eligibility: Week 6. Week 7 and Week 9 are planning stress tests, not confirmed return dates.'}];
export function attachInjuryEvidence(players,season,overrides=[]){
  const records=[...INITIAL,...overrides].filter(r=>r.season===Number(season));
  return players.map(p=>({...p,injuryEvidence:records.findLast(r=>r.playerId===String(p.playerId))||null}));
}
export function validateInjuryEvidence(input,season){
  if(!input||!/^\d{1,12}$/.test(String(input.playerId)))throw new Error('Choose a rostered player.');
  const weeks=['earliestWeek','earlyWeek','planningWeek','lateWeek'].map(k=>Number(input[k]));
  if(weeks.some(w=>!Number.isInteger(w)||w<1||w>19)||weeks.some((w,i)=>i&&w<weeks[i-1]))throw new Error('Return weeks must be ordered from 1 to 19 (19 means beyond this season).');
  const url=new URL(input.sourceUrl);if(url.protocol!=='https:'||url.username||url.password)throw new Error('Use a public HTTPS source link.');
  return {playerId:String(input.playerId),season:Number(season),...Object.fromEntries(['earliestWeek','earlyWeek','planningWeek','lateWeek'].map((k,i)=>[k,weeks[i]])),sourceUrl:url.href,
    note:String(input.note||'User-reviewed return scenarios; not a medical forecast.').slice(0,500),checkedAt:new Date().toISOString(),reviewBy:new Date(Date.now()+3*86400000).toISOString()};
}
