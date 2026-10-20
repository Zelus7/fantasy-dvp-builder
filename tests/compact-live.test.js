import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {waiverCard,waiverSummary} from '../public/ui.js';
import {readRoute,writeRoute,filterRecommendations} from '../public/navigation.js';

const player={playerId:'21',name:'Target Receiver',position:'WR',team:'BUF',mode:'week',lineupGain:1.2,waiverValue:1.2,median:12,drop:{name:'Bench Receiver',position:'WR',median:10},dropCost:2,dropCostWeeks:[8,9],dropCostComplete:true,comparison:[]};

test('compact waiver summary puts both player names and gain before expandable evidence',()=>{
  const html=waiverCard(player),summary=html.split('</summary>')[0];
  assert.match(summary,/Target Receiver/);assert.match(summary,/Bench Receiver/);assert.match(summary,/\+1.2/);
  assert.doesNotMatch(html,/data-decision="21" open/);
  assert.match(html,/2.0 modeled starter points/);assert.match(html,/not an extra deduction/);
  assert.match(html,/Nothing is submitted to ESPN/);
});
test('injury cover is explicitly conditional and never replaces the normal-lineup gain',()=>{
  const p={...player,lineupGain:0,contingencies:[{name:'Questionable Starter',gain:2.3}]};
  assert.deepEqual(waiverSummary(p),{gain:2.3,label:'if Questionable Starter is out',verdict:'Injury cover'});
  const summary=waiverCard(p).split('</summary>')[0];
  assert.match(summary,/if Questionable Starter is out/);assert.match(summary,/\+0.0 normal-lineup gain/);
});
test('future view uses window total, labels unknown coverage, and does not reuse injury-cover gain',()=>{
  const p={...player,mode:'bridge',waiverValue:8.3,contingencies:[{name:'Starter',gain:3}]};
  assert.equal(waiverSummary(p).gain,8.3);assert.match(waiverSummary(p).label,/total/);
  assert.equal(waiverSummary({...p,complete:false}).gain,null);
  assert.match(waiverCard({...p,dropCostComplete:false}),/Future starter loss is not fully known/);
});
test('compact content escapes names, search, and IDs',()=>{
  const html=waiverCard({...player,name:'<script>x</script>',playerId:'"><svg onload=x>'});
  assert.doesNotMatch(html,/<script>|<svg/);assert.match(html,/&lt;script&gt;/);
});
test('live routes roundtrip views, horizons, filters, search, week, and expansion',()=>{
  const route={current:'waivers',mode:'ros',tradeMode:'week',position:'D/ST',search:'New York & Jets',week:7,expanded:['-16020','4047646']};
  assert.deepEqual(readRoute(writeRoute(route)),route);
  assert.deepEqual(readRoute('#unknown?mode=hack&position=XXX&week=999&open=%3Cscript%3E'),readRoute(''));
  assert.equal(filterRecommendations([player],'bench receiver').length,1);
  assert.equal(filterRecommendations([player],'missing').length,0);
});
test('production shell loads real app components, not fictional preview data',()=>{
  const html=readFileSync('public/index.html','utf8'),app=readFileSync('public/app.js','utf8');
  assert.match(html,/compact.css/);assert.match(html,/main-content/);
  assert.doesNotMatch(html,/design-preview|preview-data/);assert.doesNotMatch(app,/preview-data/);
  assert.ok(app.indexOf('<div id="waiver-results">')<app.indexOf("panel('claim-plan-panel'"));
  assert.match(app,/mountClaimPlanner\(section.querySelector\('#claim-plan'\)/);
});
