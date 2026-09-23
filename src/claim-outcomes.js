// Report text is supplied by the user, never an independently verified API receipt.
const clean=value=>String(value??'').replace(/[’‘]/g,"'").replace(/\s+/g,' ').trim();
const same=(a,b)=>clean(a).toLowerCase()===clean(b).toLowerCase();
const day=value=>new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(value));
export function parseOfferReport(text){
  if(typeof text!=='string'||text.length>24000)throw new Error('Paste an ESPN Offers Report table under 24,000 characters.');
  const lines=text.split(/[\t\r\n]+/).map(clean).filter(Boolean),rows=[];
  for(let i=2;i<lines.length-1;i++){
    if(!/^\$\d+$/.test(lines[i]))continue;
    const player=lines[i-1].match(/^(.+) ([A-Z]{2,4}), (QB|RB|WR|TE|K|D\/ST)$/),result=lines[i+1];
    if(!player||! /^(Added\.|Unsuccessful\.)/.test(result))continue;
    const amount=Number(lines[i].slice(1));if(!Number.isSafeInteger(amount)||amount>100000)continue;
    const drop=result.match(/dropped\s+(.+),\s+[A-Z]{2,4}\s+(?:QB|RB|WR|TE|K|D\/ST)\s+to Waivers\.?$/);
    rows.push({teamName:lines[i-2],playerName:player[1],position:player[2],bid:amount,
      status:result.startsWith('Added.')?'won':/already been dropped/i.test(result)?'skipped':/already been added to another team/i.test(result)?'outbid':'unsuccessful',
      dropName:drop?.[1]||null,reason:result.slice(0,500)});
  }
  if(!rows.length)throw new Error('No recognizable offer rows. Copy the table including Team, Claim, Offer and Results.');
  if(rows.length>150)throw new Error('Import one processing date at a time (at most 150 offers).');
  return rows;
}

export function reviewOfferReport(input,plan,teamName,now=Date.now()){
  if(!plan?.version||!plan.claims?.length)throw new Error('Save a nonempty claim plan before importing its outcomes.');
  const reportDate=input.reportDate;
  if(!/^\d{4}-\d{2}-\d{2}$/.test(reportDate||'')||!Number.isFinite(Date.parse(reportDate))||new Date(reportDate).toISOString().slice(0,10)!==reportDate||reportDate>day(now)||reportDate<day(plan.updatedAt))throw new Error('Choose the actual ESPN processing date, on or after this plan was saved and not in the future (Eastern time).');
  const rows=parseOfferReport(input.text),claims=[];
  for(const claim of plan.claims){
    const matches=rows.filter(r=>same(r.teamName,teamName)&&same(r.playerName,claim.addName));
    if(matches.length!==1)throw new Error(`Expected exactly one ${claim.addName} result for ${teamName}. Copy the full report for the correct date.`);
    const row=matches[0];
    if(row.bid!==claim.bid)throw new Error(`${claim.addName}: report bid differs from the saved plan. Do not attach a different claim's result.`);
    if(row.status==='won'&&(claim.dropId?!same(row.dropName,claim.dropName):row.dropName!=null))throw new Error(`${claim.addName}: reported drop does not match the saved conditional drop.`);
    const rivals=rows.filter(r=>same(r.playerName,claim.addName)&&!same(r.teamName,teamName));
    claims.push({...claim,status:row.status,paid:row.status==='won'?row.bid:0,reason:row.reason,
      competingOffers:rivals.map(r=>({teamName:r.teamName,bid:r.bid,status:r.status})),
      highestOtherBid:rivals.length?Math.max(...rivals.map(r=>r.bid)):null});
  }
  const winners=claims.filter(c=>c.status==='won'),drops=winners.map(c=>c.dropId).filter(Boolean);
  if(new Set(drops).size!==drops.length)throw new Error('Two reported wins use the same conditional drop. Review the report and claim sequence.');
  return {reportDate,claims,totalPaid:winners.reduce((sum,c)=>sum+c.paid,0),source:'Pasted ESPN Offers Report — user-supplied, not independently verified by the app',
    caveat:'Competing bids are only those listed in the pasted report. They are hindsight evidence, not a prediction of future prices or proof that a smaller bid would have won.'};
}

export function reconcileClaimPlan(plan,roster,{stale=false,updatedAt=null}={},receipt=null,now=Date.now()){
  const age=now-Date.parse(updatedAt),fresh=!stale&&Number.isFinite(age)&&age>=0&&age<=300000;
  const owned=new Set(roster.map(p=>String(p.playerId)));
  return {checkedAt:updatedAt,fresh,receipt,claims:(plan.claims||[]).map(c=>{
    const result=receipt?.claims.find(r=>r.addId===c.addId),onRoster=fresh?owned.has(c.addId):null;
    return {...c,outcome:result?.status||'unresolved',onRoster,
      observation:!fresh?'Roster check unavailable or stale':onRoster?'Currently on your roster — acquisition method and price are not proved by ownership':owned.has(c.dropId)?'Target is not on your roster; conditional drop is still rostered':'Target and conditional drop are absent; check the processed report',
      discrepancy:!!result&&fresh&&((result.status==='won')!==onRoster)};
  })};
}
