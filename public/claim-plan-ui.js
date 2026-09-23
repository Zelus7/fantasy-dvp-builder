import {esc,date,warnings} from './ui.js';
import {claimPlanSummary} from './model/claim-plan.js';

const drafts=new Map();
window.addEventListener('claim-outcomes-saved',event=>{const draft=drafts.get(event.detail?.key);if(draft?.data&&!draft.dirty&&!draft.busy)void load(draft,false);});
const copy=value=>structuredClone(value);
const option=(value,label,selected)=>`<option value="${esc(value)}"${String(value)===String(selected)?' selected':''}>${esc(label)}</option>`;
function draftFor(key){if(!drafts.has(key))drafts.set(key,{data:null,edit:null,busy:false,error:'',dirty:false});return drafts.get(key);}
export function clearClaimDrafts(){drafts.clear();}
export function mountClaimPlanner(element,options){
  if(!element)return;
  const {key,api,query}=options,draft=draftFor(key);draft.element=element;draft.options=options;
  const paint=()=>render(draft);
  element.onclick=async event=>{
    const b=event.target.closest('button[data-claim]');if(!b||draft.busy)return;
    const action=b.dataset.claim;
    if(action==='load'){
      if(draft.dirty&&!confirm('Discard unsaved claim-plan edits and reload the saved plan? No ESPN claims will be changed.'))return;
      await load(draft,true);return;
    }
    if(action==='new'){
      if(!confirm('Start a new empty worksheet? Saved versions and reports remain in the audit trail. No ESPN claim is canceled or changed.'))return;
      draft.busy=true;draft.error='';paint();
      try{const result=await api(`/api/claim-plan?${query()}`,{method:'PUT',body:JSON.stringify({version:draft.edit.version,week:draft.data.context.liveWeek,claims:[],spendingLimit:0})});draft.data=result;draft.edit=copy(result.plan);draft.dirty=false;}
      catch(error){draft.error=error.message}finally{draft.busy=false;paint()}return;
    }
    if(action==='add'){if(draft.edit.claims.length>=12)return;draft.edit.claims.push({addId:'',dropId:null,bid:draft.data.context.market.minimumBid??1,reportedPending:false});draft.dirty=true;paint();return;}
    const index=Number(b.dataset.index);
    if(action==='remove')draft.edit.claims.splice(index,1);
    if(action==='up'&&index>0)[draft.edit.claims[index-1],draft.edit.claims[index]]=[draft.edit.claims[index],draft.edit.claims[index-1]];
    if(action==='down'&&index<draft.edit.claims.length-1)[draft.edit.claims[index+1],draft.edit.claims[index]]=[draft.edit.claims[index],draft.edit.claims[index+1]];
    draft.dirty=true;paint();
  };
  element.oninput=event=>{
    const field=event.target.dataset.field;if(!field||!draft.edit)return;
    if(field==='limit')draft.edit.spendingLimit=event.target.value===''?null:Number(event.target.value);
    else{const row=draft.edit.claims[Number(event.target.dataset.index)];row[field]=field==='bid'?(event.target.value===''?null:Number(event.target.value)):field==='reportedPending'?event.target.checked:event.target.value||null;}
    draft.dirty=true;const status=element.querySelector('[data-draft-status]');if(status)status.textContent='Unsaved edits — save to recalculate and check the full plan.';
  };
  element.onsubmit=async event=>{
    event.preventDefault();if(draft.busy||!draft.edit)return;draft.busy=true;draft.error='';paint();
    try{const result=await api(`/api/claim-plan?${query()}`,{method:'PUT',body:JSON.stringify({...draft.edit,week:draft.data.context.liveWeek})});draft.data=result;draft.edit=copy(result.plan);draft.dirty=false;}
    catch(error){draft.error=error.message;}
    finally{draft.busy=false;paint();}
  };
  paint();if(!draft.data&&!draft.busy&&!draft.error)void load(draft,false);
}
async function load(draft,force){
  draft.busy=true;draft.error='';render(draft);
  try{const result=await draft.options.api(`/api/claim-plan?${draft.options.query({force})}`);draft.data=result;draft.edit=copy(result.plan);draft.dirty=false;}
  catch(error){draft.error=error.message;}
  finally{draft.busy=false;render(draft);}
}
export async function queueRecommendation(player,options){
  const draft=draftFor(options.key);draft.options=options;
  if(!draft.data){if(draft.busy)throw new Error('The claim plan is loading. Try Add to plan again shortly.');await load(draft,false);}
  if(!draft.data)throw new Error(draft.error||'Claim plan unavailable.');
  if(draft.data.processedReport)throw new Error('This worksheet has processed results. Start a new worksheet first; your previous claims and report will stay in history.');
  if(draft.edit.claims.some(c=>c.addId===String(player.playerId)))throw new Error('That player is already in your plan.');
  if(draft.edit.claims.length>=12)throw new Error('The plan supports at most 12 claims.');
  draft.edit.claims.push({addId:String(player.playerId),dropId:player.drop?String(player.drop.playerId):null,bid:player.faab?.low??draft.data.context.market.minimumBid??1,reportedPending:false});draft.dirty=true;render(draft);
}
function render(draft){
  const element=draft.element;if(!element?.isConnected)return;
  const {data,edit,busy,error,dirty}=draft;
  const intro='<div class="eyebrow">PLAN → REVIEW → ESPN</div><h2>Saved claim plan</h2><p>Choose exact bids and conditional drops. Saving here <strong>does not submit, edit, reorder or cancel anything in ESPN</strong>. The connector still syncs data only. Complete the final transaction in ESPN and verify Pending Moves.</p>';
  if(!data){element.innerHTML=`<article class="card">${intro}${error?warnings([error]):'<p>Loading your saved plan…</p>'}<button class="secondary" data-claim="load" ${busy?'disabled':''}>Load plan</button></article>`;return;}
  const {context,review,plan}=data,summary=claimPlanSummary(edit.claims.filter(c=>Number.isFinite(c.bid)));
  if(data.processedReport){element.innerHTML=`<article class="card claim-planner"><h2>Worksheet completed</h2><p>Plan v${plan.version} has a saved processed report dated ${esc(data.processedReport.reportDate)}. These rows are kept as history, not treated as pending or re-submitted.</p>${error?warnings([error]):''}<button class="secondary" data-claim="new" ${busy?'disabled':''}>Start new worksheet (keep history)</button><button class="secondary" data-claim="load" ${busy?'disabled':''}>Reload saved plan</button></article>`;return;}
  const candidates=[...context.freeAgents].sort((a,b)=>a.name.localeCompare(b.name));
  const labels=(players,selected,empty)=>option('',empty,selected)+(!players.some(p=>p.playerId===selected)&&selected?option(selected,`${plan.claims.find(c=>c.addId===selected)?.addName||plan.claims.find(c=>c.dropId===selected)?.dropName||selected} — no longer in current pool`,selected):'')+players.map(p=>option(p.playerId,`${p.name} · ${p.position}`,selected)).join('');
  element.innerHTML=`<article class="card claim-planner">${intro}<p>ESPN FAAB remaining: <strong>${context.market.remaining==null?'Unknown':`$${context.market.remaining}`}</strong> · Minimum bid: ${context.market.minimumBid==null?'Unknown':`$${context.market.minimumBid}`} · W${context.liveWeek}</p><p class="fine">${esc(context.coverage)}</p>${error?warnings([error]):''}<form><fieldset ${busy?'disabled':''}><label>Your maximum spend across successful claims<input data-field="limit" aria-label="Plan spending limit" type="number" min="0" max="100000" step="1" value="${edit.spendingLimit??''}" required></label><p data-draft-status aria-live="polite">${dirty?'Unsaved edits — save to recalculate and check the full plan.':`Saved ${date(plan.updatedAt)}. This is a worksheet, not an ESPN receipt.`}</p><div class="stack">${edit.claims.map((c,i)=>`<section class="claim-row"><h3>Priority ${i+1}</h3><label>Add target<select aria-label="Claim ${i+1} add target" data-field="addId" data-index="${i}" required>${labels(candidates,c.addId,'Choose a player')}</select></label><label>Exact bid<input aria-label="Claim ${i+1} bid" data-field="bid" data-index="${i}" type="number" min="${context.market.minimumBid??0}" max="${context.market.remaining??100000}" step="1" value="${c.bid??''}" required></label><label>Conditional drop<select aria-label="Claim ${i+1} conditional drop" data-field="dropId" data-index="${i}">${labels(context.roster,c.dropId,'Use an open roster slot')}</select></label><label class="checkbox"><input data-field="reportedPending" data-index="${i}" type="checkbox" ${c.reportedPending?'checked':''}> I already see this exact claim in ESPN Pending Moves (self-reported)</label><div class="button-row"><button type="button" class="secondary" data-claim="up" data-index="${i}" ${i===0?'disabled':''}>Move up</button><button type="button" class="secondary" data-claim="down" data-index="${i}" ${i===edit.claims.length-1?'disabled':''}>Move down</button><button type="button" class="secondary" data-claim="remove" data-index="${i}">Remove from worksheet</button></div></section>`).join('')||'<p>No claims in this worksheet. Existing ESPN offers are not automatically imported.</p>'}</div><div class="button-row"><button type="button" class="secondary" data-claim="add" ${edit.claims.length>=12?'disabled':''}>Add claim</button><button type="submit" class="primary">Save & check plan</button><button type="button" class="secondary" data-claim="load">Reload & recheck saved plan</button></div></fieldset></form><p><strong>${dirty?'Draft':'Saved'} maximum successful spend: $${summary.maximumSpend}.</strong> Shared conditional drops are mutually exclusive; separate drops are cumulative. Includes this worksheet only.</p>${!dirty?`${warnings(review.errors)}${warnings(review.warnings)}<p>Last check ${esc(date(review.checkedAt))}. ${review.valid?'Loaded roster/budget/availability checks passed; ESPN still controls acceptance and processing.':'Needs review before acting.'}</p>`:''}<a href="${esc(data.espnUrl)}" target="_blank" rel="noopener noreferrer">Open your ESPN team / Pending Moves</a><p class="fine">Before final confirmation, match add, conditional drop, bid, priority and processing time. Do not submit a duplicate of an already-pending claim. Nothing here is an automatic spending authorization.</p></article>`;
  const reset=document.createElement('button');reset.type='button';reset.className='secondary';reset.dataset.claim='new';reset.textContent='Start new worksheet (keep history)';reset.disabled=busy; element.querySelector('form fieldset > .button-row').append(reset);
}
