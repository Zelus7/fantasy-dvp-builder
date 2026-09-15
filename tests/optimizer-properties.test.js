import test from 'node:test';
import assert from 'node:assert/strict';
import {maximizeLegalAssignment,eligibleForSlot,optimizeLineup} from '../src/analysis.js';
function brute(players,slots,index=0,used=new Set()){
  if(index===slots.length)return {filled:0,score:0};
  let best=brute(players,slots,index+1,used);
  for(let i=0;i<players.length;i++)if(!used.has(i)&&eligibleForSlot(players[i],slots[index].slotId)){
    used.add(i);const rest=brute(players,slots,index+1,used);used.delete(i);
    const value={filled:rest.filled+1,score:rest.score+players[i].decisionScore};
    if(value.filled>best.filled||value.filled===best.filled&&value.score>best.score)best=value;
  }return best;
}
test('legal optimizer matches exhaustive search across 200 deterministic small rosters',()=>{
  let seed=7;const random=()=>((seed=(seed*1664525+1013904223)>>>0)/2**32);
  for(let trial=0;trial<200;trial++){
    const slots=[{slotId:2},{slotId:4},{slotId:23}];
    const players=Array.from({length:1+Math.floor(random()*6)},(_,i)=>({playerId:String(i),eligibleSlotIds:random()<.5?[2,23]:[4,23],decisionScore:Math.floor(random()*30)-5}));
    const assignment=maximizeLegalAssignment(players,slots),actual=assignment.filter(i=>i>=0),expected=brute(players,slots);
    assert.equal(new Set(actual).size,actual.length);assert.equal(actual.length,expected.filled);
    assert.equal(actual.reduce((s,i)=>s+players[i].decisionScore,0),expected.score);
  }
});
test('explicit flex-only eligibility does not invent a positional slot',()=>{
  const p={position:'WR',eligibleSlotIds:[23],decisionScore:10};
  assert.equal(eligibleForSlot(p,4),false);assert.deepEqual(maximizeLegalAssignment([p],[{slotId:4},{slotId:23}]),[-1,0]);
});
test('change explanations pair the incoming quarterback with the outgoing quarterback',()=>{
  const players=[{playerId:'old-wr',position:'WR',lineupSlotId:4,eligibleSlotIds:[4],isStarter:true,decisionScore:1},{playerId:'old-qb',position:'QB',lineupSlotId:0,eligibleSlotIds:[0],isStarter:true,decisionScore:2},{playerId:'new-qb',position:'QB',lineupSlotId:20,eligibleSlotIds:[0],decisionScore:20},{playerId:'new-wr',position:'WR',lineupSlotId:20,eligibleSlotIds:[4],decisionScore:15}];
  const result=optimizeLineup(players,{0:1,4:1},players,0);
  assert.equal(result.changes.length,2);assert.equal(result.changes.find(c=>c.start.playerId==='new-qb').bench.playerId,'old-qb');
});
