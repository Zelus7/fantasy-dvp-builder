import {optimizeLineup} from './analysis.js';
import {STARTER_SLOT_IDS} from './constants.js';

const round=value=>Math.round(value*10)/10;
const id=player=>String(player.playerId);
const finite=value=>value!=null&&Number.isFinite(Number(value));
const unavailable=player=>['OUT','IR','INJURY_RESERVE','SUSPENDED','SUSPENSION','DOUBTFUL'].includes(String(player.injuryStatus).toUpperCase());
export const DECISION_METHOD='roster-fit-v2';
export function byeCoverage(roster,slots,schedules){
  const weeks=[...new Set(Object.values(schedules).flatMap(s=>(s.weeks||[]).map(w=>w.week)))].sort((a,b)=>a-b);
  return weeks.map(week=>{
    const byes=[],missing=[];
    const players=roster.map(p=>{const game=schedules[id(p)]?.weeks.find(w=>w.week===week),value=horizonValue(p,'ros');if(game?.bye)byes.push(p.name);if(!game||game.missingSchedule)missing.push(p.name);return{...p,game:null,isStarter:false,decisionScore:value??0,hasEstimate:value!=null,isAvailable:value!=null&&!!game&&!game.bye&&!game.missingSchedule}});
    const lineup=optimizeLineup(players,slots,[],0);
    return{week,byePlayers:byes,missingPlayers:missing,unfilledSlots:lineup.unfilledSlots.map(s=>s.slotId),status:missing.length?'unknown':lineup.unfilledSlots.length?'coverage-gap':'covered'};
  });
}

export function horizonValue(player, mode='week', schedule=null) {
  if(mode==='week')return player.eligibleForRecommendation===false||player.isAvailable===false||!finite(player.median)?null:Number(player.median);
  if(unavailable(player)||player.team==='FA'||player.proTeam==='FA')return null;
  const historical=finite(player.feature?.seasonPpg)&&Number(player.feature?.games)>0?Number(player.feature.seasonPpg):null;
  const projection=finite(player.seasonProjectedPoints)?Number(player.seasonProjectedPoints)/17:finite(player.projectedPoints)?Number(player.projectedPoints):null;
  if(historical==null&&projection==null)return null;
  // A transparent baseline, not a market price or a calibrated ROS forecast.
  // Do not let one volatile week dominate established production.
  const historicalWeight=Math.min(.65,Number(player.feature?.currentGames||0)/10);
  const base=projection==null?historical:historical==null?projection:projection*(1-historicalWeight)+historical*historicalWeight;
  const matchup=finite(schedule?.restOfSeasonPercentile)?Math.max(-.03,Math.min(.03,(Number(schedule.restOfSeasonPercentile)-50)/50*.03)):0;
  return Math.max(0,base*(1+matchup));
}

function valued(roster,mode,schedules={}) {
  return roster.map(player=>{
    const value=horizonValue(player,mode,schedules[id(player)]);
    return {...player,decisionScore:value??0,hasEstimate:value!=null,isAvailable:value!=null,
      // ROS lineup comparison ignores this week's game locks, not injuries.
      ...(mode==='ros'?{game:null}:{}),horizonValue:value};
  });
}

export function rosterStrength(roster,slots,mode='week',schedules={},now=Date.now()) {
  const players=valued(roster,mode,schedules);
  const lineup=optimizeLineup(players,slots,mode==='week'?players:[],mode==='week'?now:0);
  const expectedSlots=Object.entries(slots||{}).reduce((sum,[slot,count])=>sum+(STARTER_SLOT_IDS.has(Number(slot))?Number(count):0),0);
  const starterIds=new Set(lineup.starters.map(id));
  const starterValue=lineup.starters.reduce((sum,p)=>sum+(p.horizonValue??0),0);
  const depthValue=players.filter(p=>!starterIds.has(id(p))).reduce((sum,p)=>sum+(p.horizonValue??0),0);
  return {starterValue:round(starterValue),depthValue:round(depthValue),unfilled:Math.max(0,expectedSlots-lineup.starters.length),lineup};
}

function locked(player,now){return player.game?.kickoff&&Date.parse(player.game.kickoff)<=now;}
function dropAllowed(player,protectedIds,mode,now){return !protectedIds.has(id(player))&&!player.cantCut&&!unavailable(player)&&!(mode==='week'&&locked(player,now));}
function names(players){return players.map(p=>p.name).join(' + ');}

export function recommendWaiverMoves(freeAgents,roster,{mode='week',slots={},schedules={},protectedIds=[],rosterCapacity=null,limit=20,now=Date.now()}={}) {
  if(!['week','ros'].includes(mode))throw new Error('Unsupported waiver horizon');
  const protectedSet=new Set(protectedIds.map(String)),owned=new Set(roster.map(id));
  const before=rosterStrength(roster,slots,mode,schedules,now),beforeRos=rosterStrength(roster,slots,'ros',schedules,now);
  const activeCount=roster.filter(p=>Number(p.lineupSlotId)!==21).length;
  const drops=Number(rosterCapacity)>activeCount?[null]:roster.filter(p=>dropAllowed(p,protectedSet,mode,now));
  const candidates=freeAgents.filter(p=>!owned.has(id(p))&&['FREEAGENT','WAIVERS'].includes(p.status)&&Number(p.onTeamId||0)<=0&&horizonValue(p,mode,schedules[id(p)])!=null&&!(mode==='week'&&locked(p,now)));
  const output=[];
  for(const add of candidates){
    const moves=[];
    for(const drop of drops){
      const afterRoster=[...roster.filter(p=>!drop||id(p)!==id(drop)),{...add,isStarter:false,lineupSlotId:20}];
      const after=rosterStrength(afterRoster,slots,mode,schedules,now);
      const afterRos=mode==='ros'?after:rosterStrength(afterRoster,slots,'ros',schedules,now);
      if(after.unfilled>before.unfilled||afterRos.unfilled>beforeRos.unfilled)continue;
      const lineupGain=round(after.starterValue-before.starterValue);
      const depthGain=round(after.depthValue-before.depthValue);
      const seasonCost=round(afterRos.starterValue-beforeRos.starterValue);
      // Do not burn a strong long-term starter for a small one-week stream.
      if(mode==='week'&&seasonCost<-.5)continue;
      const edge=round(lineupGain+.15*depthGain);
      if(lineupGain<-.1||edge<=.5)continue;
      const kind=lineupGain>=.5?'Lineup upgrade':'Depth upgrade';
      const evidence=[`${kind}: ${lineupGain>=0?'+':''}${lineupGain.toFixed(1)} estimated ${mode==='week'?'this-week':'typical-week'} optimized starter points.`,
        `Add ${add.name}; ${drop?`drop ${drop.name}`:'use your open roster slot'}. ${depthGain>=0?'+':''}${depthGain.toFixed(1)} aggregate bench-baseline change.`,
        ...(add.reasons||[]).slice(0,2)];
      moves.push({drop,lineupGain,depthGain,seasonCost,edge,kind,evidence});
    }
    moves.sort((a,b)=>b.lineupGain-a.lineupGain||b.edge-a.edge);
    const best=moves[0];if(!best)continue;
    output.push({...add,mode,waiverValue:best.edge,kind:best.kind,drop:best.drop,
      lineupGain:best.lineupGain,depthGain:best.depthGain,seasonCost:best.seasonCost,
      horizonEstimate:round(horizonValue(add,mode,schedules[id(add)])),schedule:schedules[id(add)]||null,
      reasons:best.evidence,alternatives:moves.slice(1,3).map(m=>({drop:m.drop,lineupGain:m.lineupGain,waiverValue:m.edge})),
      warnings:[...(add.warnings||[]),'Estimates are not guaranteed points. Verify current availability and league transaction rules in ESPN.'],
      faab:null,faabExplanation:'No personalized bid until remaining budget and league bidding rules are verified.',
      method:DECISION_METHOD});
  }
  return output.sort((a,b)=>b.lineupGain-a.lineupGain||b.waiverValue-a.waiverValue).slice(0,Math.min(30,Math.max(1,limit)));
}

export function evaluateBilateralTrade(yourRoster,theirRoster,giveIds,receiveIds,{slots={},schedules={},protectedIds=[]}={}) {
  if(!Array.isArray(giveIds)||!Array.isArray(receiveIds)||giveIds.length>2||receiveIds.length>2)return {viable:false,recommendation:'not-recommended',reason:'Select one or two players on each side.',give:[],receive:[],method:DECISION_METHOD};
  const giveSet=new Set(giveIds.map(String)),receiveSet=new Set(receiveIds.map(String));
  const give=yourRoster.filter(p=>giveSet.has(id(p))),receive=theirRoster.filter(p=>receiveSet.has(id(p)));
  const rejected=reason=>({viable:false,recommendation:'not-recommended',reason,give,receive,method:DECISION_METHOD});
  if(!give.length||!receive.length||give.length!==giveSet.size||receive.length!==receiveSet.size)return rejected('Every player must belong to the selected side.');
  if(giveIds.length!==giveSet.size||receiveIds.length!==receiveSet.size)return rejected('Duplicate players are not allowed.');
  if(give.some(p=>receiveSet.has(id(p))))return rejected('The same player cannot appear on both sides.');
  if(give.length!==receive.length)return rejected('Unequal packages need a verified extra drop or open roster slot; automatic suggestions use equal-sized packages.');
  if(give.some(p=>protectedIds.map(String).includes(id(p))))return rejected('A protected player is included.');
  if([...give,...receive].some(p=>horizonValue(p,'ros',schedules[id(p)])==null))return rejected('A player has unresolved availability or insufficient valuation data.');
  const total=players=>players.reduce((sum,p)=>sum+horizonValue(p,'ros',schedules[id(p)]),0);
  const giveValue=total(give),receiveValue=total(receive),imbalance=Math.abs(giveValue-receiveValue)/Math.max(giveValue,receiveValue,1);
  if(imbalance>.2)return rejected('More than 20% baseline-value imbalance: not a balanced offer under this screening model.');
  const yoursAfter=[...yourRoster.filter(p=>!giveSet.has(id(p))),...receive];
  const theirsAfter=[...theirRoster.filter(p=>!receiveSet.has(id(p))),...give];
  const yoursBefore=rosterStrength(yourRoster,slots,'ros',schedules),theirsBefore=rosterStrength(theirRoster,slots,'ros',schedules);
  const yours=rosterStrength(yoursAfter,slots,'ros',schedules),theirs=rosterStrength(theirsAfter,slots,'ros',schedules);
  if(yours.unfilled>yoursBefore.unfilled||theirs.unfilled>theirsBefore.unfilled)return rejected('The exchange leaves a team unable to fill its starting lineup.');
  const yourGain=round(yours.starterValue-yoursBefore.starterValue),theirGain=round(theirs.starterValue-theirsBefore.starterValue);
  const viable=yourGain>=.5&&theirGain>=.5;
  const describe=(before,after)=>({before:before.starterValue,after:after.starterValue,gain:round(after.starterValue-before.starterValue),benchChange:round(after.depthValue-before.depthValue)});
  return {viable,recommendation:viable?'mutual-fit':'not-recommended',give,receive,giveValue:round(giveValue),receiveValue:round(receiveValue),
    yourImpact:describe(yoursBefore,yours),theirImpact:describe(theirsBefore,theirs),imbalance:round(imbalance*100),
    reason:viable?'Both legal starting lineups improve with comparable exchanged baseline value.':'The model does not show a meaningful starting-lineup improvement for both teams.',
    reasons:[`You send ${names(give)} and receive ${names(receive)}.`,
      `Your typical-week starter estimate: ${yoursBefore.starterValue} → ${yours.starterValue}.`,
      `Their typical-week starter estimate: ${theirsBefore.starterValue} → ${theirs.starterValue}.`],
    warnings:['This is a roster-fit screen, not market consensus or an acceptance probability.','ROS estimates use ESPN season projections when available (per scheduled game) and observed production; weekly projections are a labeled fallback.','The 20% balance and 0.5-point gain cutoffs are screening rules, not learned acceptance thresholds.'],
    method:DECISION_METHOD};
}

export function discoverTradeTargets(teams,yourTeamId,{slots={},schedules={},protectedIds=[],limit=8}={}) {
  const yours=teams.find(t=>String(t.id)===String(yourTeamId));if(!yours)return [];
  const eligible=roster=>roster.filter(p=>['QB','RB','WR','TE'].includes(p.position)&&horizonValue(p,'ros',schedules[id(p)])!=null)
    .sort((a,b)=>horizonValue(b,'ros',schedules[id(b)])-horizonValue(a,'ros',schedules[id(a)])).slice(0,8);
  const bundles=players=>{const out=players.map(p=>[p]);for(let i=0;i<players.length;i++)for(let j=i+1;j<players.length;j++)out.push([players[i],players[j]]);return out;};
  const own=bundles(eligible(yours.roster).filter(p=>!protectedIds.map(String).includes(id(p)))),proposals=[];
  const total=players=>players.reduce((sum,p)=>sum+horizonValue(p,'ros',schedules[id(p)]),0);
  for(const team of teams.filter(t=>String(t.id)!==String(yourTeamId))){
    const pairs=[];
    for(const give of own)for(const receive of bundles(eligible(team.roster))){
      if(give.length!==receive.length)continue;
      const diff=Math.abs(total(give)-total(receive))/Math.max(total(give),total(receive),1);
      if(diff<=.2&&give.some(p=>!receive.every(r=>r.position===p.position)))pairs.push({give,receive,diff});
    }
    // Bound CPU cost; evaluate the most balanced offers first, retaining both
    // singles and packages. No claim to search every possible league trade.
    const shortlist=[...pairs.filter(p=>p.give.length===1).sort((a,b)=>a.diff-b.diff).slice(0,12),...pairs.filter(p=>p.give.length===2).sort((a,b)=>a.diff-b.diff).slice(0,12)];
    for(const pair of shortlist){const result=evaluateBilateralTrade(yours.roster,team.roster,pair.give.map(id),pair.receive.map(id),{slots,schedules,protectedIds});if(result.viable)proposals.push({...result,otherTeam:{id:team.id,name:team.name}});}
  }
  return proposals.sort((a,b)=>Math.min(b.yourImpact.gain,b.theirImpact.gain)-Math.min(a.yourImpact.gain,a.theirImpact.gain)).slice(0,limit);
}
