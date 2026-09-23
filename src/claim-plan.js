// A saved review worksheet, never an ESPN transaction or execution credential.
const id=value=>String(value??'');
const integer=value=>typeof value==='number'&&Number.isSafeInteger(value)&&value>=0;
export function claimPlanSummary(claims=[]){
  const groups=new Map();
  for(const [i,c] of claims.entries()){
    const key=c.dropId?`drop:${c.dropId}`:`open:${i}`;
    const group=groups.get(key)||{dropId:c.dropId||null,claims:[],maximum:0};
    group.claims.push(c);group.maximum=Math.max(group.maximum,c.bid);groups.set(key,group);
  }
  const paths=[...groups.values()];
  return {maximumSpend:paths.reduce((n,g)=>n+g.maximum,0),paths};
}

export function validateClaimPlan(input,context={},now=Date.now()){
  const errors=[],warnings=[...(context.warnings||[])],market=context.market||{},roster=context.roster||[],free=context.freeAgents||[];
  if(!input||!Array.isArray(input.claims)||input.claims.length>12||!integer(input.spendingLimit)||input.spendingLimit>100000)return {valid:false,errors:['Use at most 12 claims and a nonnegative whole-dollar spending limit.'],warnings};
  if(context.week!==context.liveWeek||input.week!==context.liveWeek)errors.push('Claim plans must be reviewed for the current live week.');
  const age=now-Date.parse(context.verifiedAt);
  if(!Number.isFinite(age)||age<0||age>300000||context.stale)errors.push('Refresh ESPN roster, player availability and budget before saving or checking this plan.');
  if(context.scheduleReady===false)errors.push('The verified schedule is missing or stale. Refresh the schedule feed before checking game locks.');
  if(market.type!=='FAAB'||!integer(market.remaining)||!integer(market.minimumBid))errors.push('Verified FAAB balance and minimum bid are required.');
  const byId=new Map(free.map(p=>[id(p.playerId),p])),owned=new Map(roster.map(p=>[id(p.playerId),p])),seen=new Set(),claims=[];
  let openClaims=0;
  for(const [index,row] of input.claims.entries()){
    if(!row||typeof row!=='object'){errors.push(`Claim ${index+1} is invalid.`);continue;}
    const addId=id(row.addId),dropId=row.dropId==null||row.dropId===''?null:id(row.dropId),add=byId.get(addId),drop=owned.get(dropId),bid=row.bid;
    if(!/^-?\d{1,12}$/.test(addId)||seen.has(addId))errors.push(`Claim ${index+1}: choose a unique add target.`);seen.add(addId);
    if(!add||owned.has(addId)||!['FREEAGENT','WAIVERS'].includes(add.status)||Number(add.onTeamId||0)!==0)errors.push(`Claim ${index+1}: target is not confirmed available in the loaded pool.`);
    if(!integer(bid)||bid<market.minimumBid||bid>market.remaining)errors.push(`Claim ${index+1}: bid must be a whole dollar between the league minimum and remaining FAAB.`);
    if(dropId&&(!drop||drop.cantCut||(context.protectedIds||[]).map(String).includes(dropId)))errors.push(`Claim ${index+1}: drop is missing, protected, or on ESPN's can't-cut list.`);
    if(!dropId)openClaims++;
    for(const p of [add,drop].filter(Boolean)){
      const kickoff=p.game?.kickoff||context.kickoffs?.[p.proTeam];
      if(!kickoff||!Number.isFinite(Date.parse(kickoff)))errors.push(`Claim ${index+1}: ${p.name}'s game/lock time is unknown. Review in ESPN.`);
      else if(Date.parse(kickoff)<=now)errors.push(`Claim ${index+1}: ${p.name}'s game has started; review ESPN's lock rules directly.`);
    }
    if(add&&['OUT','IR','DOUBTFUL','SUSPENDED'].includes(add.injuryStatus))warnings.push(`${add.name} is ${add.injuryStatus}; this is a stash, not confirmed immediate coverage.`);
    if(drop&&['OUT','IR','DOUBTFUL','SUSPENDED'].includes(drop.injuryStatus))warnings.push(`Dropping ${drop.name} gives up his future return value. Review deliberately.`);
    if(row.reportedPending===true)warnings.push(`${add?.name||'Claim'}: you reported it pending. The app has not independently verified ESPN's pending queue.`);
    claims.push({addId,addName:add?.name||addId,position:add?.position||null,dropId,dropName:drop?.name||null,bid,reportedPending:row.reportedPending===true,priority:index+1});
  }
  const activeSize=roster.filter(p=>![21,22].includes(Number(p.lineupSlotId))).length,openSlots=Math.max(0,(context.rosterCapacity||0)-activeSize);
  if(openClaims>openSlots)errors.push(`Only ${openSlots} open roster slot(s). Name conditional drops for the other claims.`);
  const summary=claimPlanSummary(claims);
  if(summary.maximumSpend>input.spendingLimit)errors.push(`Maximum successful spend $${summary.maximumSpend} exceeds your $${input.spendingLimit} plan limit.`);
  if(summary.maximumSpend>market.remaining)errors.push('The plan can exceed your remaining FAAB.');
  for(const group of summary.paths)if(group.claims.length>1){
    warnings.push(`${group.claims.map(c=>c.addName).join(' → ')} share the same drop: at most one can succeed while that player remains on your roster. Match this order in ESPN.`);
    if(group.claims.some((c,i)=>i>0&&c.bid>group.claims[i-1].bid))warnings.push('A later fallback has a higher bid. ESPN processing/order rules may defeat your preferred sequence; review that ordering.');
  }
  warnings.push('Budget totals assume these are your only pending claims. Existing ESPN offers, roster position limits and processing deadlines must also be checked in ESPN.');
  return {valid:errors.length===0,errors:[...new Set(errors)],warnings:[...new Set(warnings)],claims,spendingLimit:input.spendingLimit,week:input.week,...summary,
    remainingAfterMaximum:integer(market.remaining)?market.remaining-summary.maximumSpend:null,checkedAt:new Date(now).toISOString()};
}
