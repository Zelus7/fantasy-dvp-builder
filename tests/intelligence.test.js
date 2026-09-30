import test from 'node:test';
import assert from 'node:assert/strict';
import {evidenceCurrent,playerIntelligence,intelligenceCoverage} from '../src/intelligence.js';
import {scheduleFallbackMessage} from '../src/experience.js';
import {analyzePlayer} from '../src/analysis.js';
import {intelligenceEvidence} from '../public/intelligence-ui.js';

const now=Date.parse('2026-09-30T12:00:00Z'),context={season:2026,week:4,now};
const dates={season:2026,fetchedAt:'2026-09-30T11:00:00Z',sourceUpdatedAt:'2026-09-30T10:00:00Z'};
const routes={...dates,routesRun:78,targetsPerRouteRun:11/78,yardsPerRouteRun:132/78,source:'fixture',sample:'season-to-date'};
test('intelligence dates reject future, missing, stale and mismatched contexts',()=>{
  assert.equal(evidenceCurrent(routes,'routes',context),true);
  for(const extra of [{season:2025},{fetchedAt:null},{sourceUpdatedAt:'2026-10-02T00:00:00Z'},{sourceUpdatedAt:'2026-09-01T00:00:00Z'}])assert.equal(evidenceCurrent({...routes,...extra},'routes',context),false);
  assert.equal(evidenceCurrent({...dates,week:3},'practice',context),false);
  assert.equal(evidenceCurrent({...dates,week:4},'practice',context),true);
});
test('fresh route and practice fetches remain explicitly partial coverage',()=>{
  const sources=intelligenceCoverage({routes:{...routes,status:'available'},practice:{...dates,week:4,status:'available',teamsWithReports:2}},[{playerId:'1',position:'WR'},{playerId:'2',position:'TE'}],{'1':{opportunity:{routes}}},context);
  assert.equal(sources.receivingUsage.status,'partial');
  assert.match(sources.receivingUsage.coverage,/1\/1 loaded WRs/);
  assert.match(sources.receivingUsage.coverage,/RB\/TE routes are unavailable/);
  assert.equal(sources.practiceReports.status,'partial');
  assert.match(sources.practiceReports.coverage,/2\/32 teams/);
  assert.equal(sources.injuryReports.status,'missing');
});
test('route metrics and availability are evidence, not manufactured recovery or extra points',()=>{
  const p={position:'WR',injuryStatus:'ACTIVE',feature:{season:2026,opportunity:{routes,
    practice:{...dates,week:4,practiceStatus:'Limited Participation in Practice'},
    injuries:{...dates,status:'Out',reportedAt:'2026-09-20T10:00:00Z'}}}};
  const result=playerIntelligence(p,context);
  assert.ok(result.items.some(i=>i.includes('14.1% targets per route')));
  assert.ok(result.warnings.some(i=>i.includes('Sources disagree')));
  assert.ok(result.warnings.some(i=>i.includes('older than three days')));
  assert.ok(result.warnings.some(i=>i.includes('does not by itself mean')));
  assert.equal(result.contextOnly,true);
  const raw={playerId:'1',name:'Fixture',position:'WR',proTeam:'BUF',projectedPoints:10,injuryStatus:'ACTIVE'};
  const base={season:2026,currentWeek:4,now,scheduleUnavailable:true,featureLookup:{'1':{season:2026,games:3,seasonPpg:9}}};
  const enriched={...base,featureLookup:{'1':{...base.featureLookup['1'],opportunity:p.feature.opportunity}}};
  assert.equal(analyzePlayer(raw,base).median,analyzePlayer(raw,enriched).median);
  assert.equal(analyzePlayer(raw,base).decisionScore,analyzePlayer(raw,enriched).decisionScore);
});
test('no report never means healthy and stale routes are not current usage',()=>{
  const missing=playerIntelligence({position:'WR',feature:{season:2026}},context);
  assert.equal(missing.items.length,0);
  assert.match(missing.warnings.join(' '),/unavailable/);
  const stale=playerIntelligence({position:'WR',feature:{season:2026,opportunity:{routes:{...routes,sourceUpdatedAt:'2025-09-30T10:00:00Z'}}}},context);
  assert.ok(!stale.items.some(i=>i.includes('78 routes')));
});
test('official out designation raises an actionable caution without guessing a return date',()=>{
  const result=playerIntelligence({position:'WR',feature:{season:2026,opportunity:{practice:{...dates,week:4,gameStatus:'Out'}}}},context);
  assert.ok(result.warnings.some(w=>w.includes('official Week 4 game designation is Out')));
});
test('browser evidence escapes reports, blocks unsafe links and preserves unknown values',()=>{
  const html=intelligenceEvidence({position:'WR',feature:{season:2026,opportunity:{
    weeklyUsage:[{week:3,targets:0,carries:null,snapShare:null,receivingAirYards:-2}],
    injuries:{...dates,status:'Out',headline:'<script>alert(1)</script>',sourceUrl:'javascript:alert(1)'}
  }}});
  assert.match(html,/0.0 targets/);assert.match(html,/— carries/);assert.match(html,/-2.0 receiving air yards/);
  assert.ok(!html.includes('<script>'));assert.ok(!html.includes('href="javascript:'));
  assert.match(html,/does not establish/);assert.match(html,/coefficients are unchanged/);
});
test('fresh schedule fallback is distinguished from unusable schedule',()=>{
  assert.match(scheduleFallbackMessage({status:'fresh',updatedAt:'2026-09-30'},[{kickoff:'2026-10-01T23:00:00Z'}]),/fresh scheduled NFL dataset/);
  assert.match(scheduleFallbackMessage({status:'fresh'},[{}]),/no fresh scheduled fallback/);
  assert.match(scheduleFallbackMessage({status:'stale'},[{}]),/no fresh scheduled fallback/);
  assert.match(scheduleFallbackMessage({status:'fresh'},[]),/no fresh scheduled fallback/);
});
