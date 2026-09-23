import {optimizeLineup} from './analysis.js';

const finite=value=>value!=null&&Number.isFinite(Number(value));
const round=value=>Math.round(value*10)/10;
const unavailable=p=>p.isAvailable===false||['OUT','IR','INJURY_RESERVE','DOUBTFUL','D','SUSPENDED','SUSPENSION'].includes(String(p.injuryStatus).toUpperCase());
const estimate=p=>finite(p.median)?Number(p.median):finite(p.projectedPoints)?Number(p.projectedPoints):null;
const scoreRoster=roster=>roster.filter(p=>![21,22].includes(Number(p.lineupSlotId))).map(p=>({...p,isAvailable:!unavailable(p),hasEstimate:estimate(p)!=null,decisionScore:estimate(p)??0}));
const total=lineup=>lineup.starters.reduce((sum,p)=>sum+p.decisionScore,0);

// These are observable incentives, not a prediction of managers' hidden bids.
// Adding the candidate to an expanded roster is an upper bound: a real rival
// must also choose a legal drop, which this app cannot choose for that manager.
export function rivalDemand(player,market={},now=Date.now()){
  const teams=Array.isArray(market.teams)?market.teams:[],slots=market.slots||{};
  if(!teams.length||!Object.keys(slots).length)return {available:false,teams:[],explanation:'Opponent roster analysis unavailable. No claim-success probability is estimated.'};
  const rivals=[];
  for(const team of teams){
    if(String(team.id)===String(market.yourTeamId)||!Array.isArray(team.roster)||!team.roster.length)continue;
    const roster=scoreRoster(team.roster),candidate=scoreRoster([{...player,lineupSlotId:20,isStarter:false}])[0];
    if(!candidate?.isAvailable||!candidate.hasEstimate)continue;
    const before=optimizeLineup(roster,slots,roster,now),after=optimizeLineup([...roster,candidate],slots,roster,now);
    const same=team.roster.filter(p=>p.position===player.position),injuries=same.filter(unavailable).map(p=>p.name);
    const uncertain=same.filter(p=>['Q','QUESTIONABLE','DTD'].includes(String(p.injuryStatus).toUpperCase())).map(p=>p.name);
    rivals.push({teamId:String(team.id),teamName:team.name,remaining:finite(team.acquisition?.remaining)?Number(team.acquisition.remaining):null,
      waiverRank:finite(team.acquisition?.waiverRank)?Number(team.acquisition.waiverRank):null,
      starterGainUpperBound:round(Math.max(0,total(after)-total(before))),couldStart:after.starters.some(p=>String(p.playerId)===String(player.playerId)),
      healthyPositionCount:same.filter(p=>!unavailable(p)).length,unavailable:injuries,questionable:uncertain,
      missingEstimates:roster.filter(p=>!p.hasEstimate&&!unavailable(p)).length});
  }
  rivals.sort((a,b)=>b.starterGainUpperBound-a.starterGainUpperBound||b.unavailable.length-a.unavailable.length||a.teamId.localeCompare(b.teamId));
  return {available:rivals.length>0,teams:rivals,plausibleStarterRivals:rivals.filter(t=>t.couldStart).length,
    explanation:'Estimated best-lineup benefit before a rival chooses a drop. Questionable is not treated as out. Budgets show purchasing capacity, not intent; bench stashes can attract bids even without a starting upgrade. No calibrated winning odds.'};
}

export function historicalBidContext(position,market={},now=Date.now()){
  const unique=new Map();
  for(const bid of market.winningBids||[]){
    const time=Date.parse(bid.at);
    if(bid.position!==position||!finite(bid.amount)||Number(bid.amount)<0||!Number.isFinite(time)||time>now||now-time>60*86400000||!bid.id)continue;
    unique.set(String(bid.id),{...bid,amount:Number(bid.amount)});
  }
  const bids=[...unique.values()].sort((a,b)=>Date.parse(b.at)-Date.parse(a.at)),amounts=bids.map(b=>b.amount).sort((a,b)=>a-b),n=amounts.length;
  return {sampleSize:n,low:n?amounts[0]:null,high:n?amounts[n-1]:null,median:n?(amounts[Math.floor((n-1)/2)]+amounts[Math.floor(n/2)])/2:null,
    recent:bids.slice(0,8),quality:n>=10?'descriptive-history':n?'small-sample':'missing',
    explanation:n?`${n} verified ${position} winning bid(s) from the last 60 days. Position matching does not make players equal in value. Winning prices do not reveal all losing bids or today's competition; this is context, not a price forecast.`:'No verified recent position-matched winning bids. Missing history is not evidence that the league bids cheaply.'};
}

export function waiverMarketContext(player,market={},now=Date.now()){
  return {history:historicalBidContext(player.position,market,now),competition:rivalDemand(player,market,now),winProbability:null};
}
