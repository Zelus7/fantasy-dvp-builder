import test from 'node:test';
import assert from 'node:assert/strict';
import {weeklyPlayerStats} from '../src/scoring.js';

const row=(source,total,extra={})=>({seasonId:2026,scoringPeriodId:2,statSplitTypeId:1,statSourceId:source,appliedTotal:total,...extra});
test('weekly ESPN scores select exact season, week and split, independent of ordering',()=>{
  const stats=[row(0,88,{seasonId:2025}),row(0,10,{scoringPeriodId:1}),row(0,200,{statSplitTypeId:0}),row(0,20),row(1,14.2),row(1,18,{scoringPeriodId:3})];
  for(const values of [stats,[...stats].reverse()]){
    const result=weeklyPlayerStats({stats:values},2026,2);
    assert.equal(result.actualPoints,20);
    assert.equal(result.projectedPoints,14.2);
  }
});
test('zero and negative actual scores are real values, missing scores are null',()=>{
  assert.equal(weeklyPlayerStats({stats:[row(0,0)]},2026,2).actualPoints,0);
  assert.equal(weeklyPlayerStats({stats:[row(0,-2)]},2026,2).actualPoints,-2);
  assert.equal(weeklyPlayerStats({stats:[row(0,null)]},2026,2).actualPoints,null);
  assert.equal(weeklyPlayerStats({stats:[row(0,12,{scoringPeriodId:1})]},2026,2).actualPoints,null);
  assert.equal(weeklyPlayerStats({},2026,2).projectedPoints,null);
});
test('conflicting duplicates and unqualified season totals cannot become weekly points',()=>{
  assert.equal(weeklyPlayerStats({stats:[row(0,10),row(0,11)]},2026,2).actualPoints,null);
  assert.equal(weeklyPlayerStats({stats:[row(0,10),row(0,10)]},2026,2).actualPoints,10);
  assert.equal(weeklyPlayerStats({stats:[{statSourceId:1,appliedTotal:250}]},2026,2).projectedPoints,null);
});
