import test from 'node:test';
import assert from 'node:assert/strict';
import {sourceFreshness} from '../src/experience.js';
import {normalizeNews} from '../src/news.js';
import {rankNews} from '../src/news-format.js';
import {number,actualNumber,playerCard,waiverCard} from '../public/ui.js';
test('freshness distinguishes missing and stale timestamps and parses D1 UTC',()=>{
  assert.equal(sourceFreshness(null,30).status,'missing');
  assert.equal(sourceFreshness('2020-01-01 00:00:00',30).status,'stale');
  assert.equal(sourceFreshness(new Date().toISOString(),30).status,'fresh');
});
test('news requires an ESPN source link and valid publication time',()=>{
  const article={headline:'NFL update',published:'2026-09-14T12:00:00Z',links:{web:{href:'https://www.espn.com/nfl/story/1'}}};
  assert.equal(normalizeNews({articles:[article]}).length,1);
  assert.equal(normalizeNews({articles:[{...article,links:{web:{href:'javascript:alert(1)'}}}]}).length,0);
});
test('news ranks roster mentions ahead of newer unrelated headlines',()=>{
  const rows=rankNews([{title:'League news',publishedAt:'2026-09-15T01:00:00Z'},{title:'Travis Kelce update',publishedAt:'2026-09-14T01:00:00Z'}],[{playerId:'1',name:'Travis Kelce'}]);
  assert.equal(rows[0].rosterMention,true);assert.equal(rows[0].title,'Travis Kelce update');
});
test('score rendering keeps missing, zero and negative values distinct',()=>{
  assert.equal(number(null),'—');assert.equal(number(0),'0.0');assert.equal(number(-2),'-2.0');
  assert.equal(actualNumber(9.82),'9.82');assert.equal(actualNumber(0),'0.00');assert.equal(actualNumber(null),'—');
  const html=playerCard({playerId:'1',name:'<script>',actualPoints:0,projectedPoints:null,median:null});
  assert.ok(html.includes('&lt;script&gt;'));assert.ok(html.includes('Actual'));assert.ok(html.includes('0.0'));assert.ok(!html.includes('% confidence'));
});
test('waiver cards explain a named drop and distinguish a vacant slot',()=>{
  const p={name:'Add',playerId:'1',drop:{name:'Drop'},horizonEstimate:5,lineupGain:1,waiverValue:2};
  assert.ok(waiverCard(p).includes('Drop Drop'));
  assert.ok(waiverCard({...p,drop:null}).includes('open roster slot'));
});
