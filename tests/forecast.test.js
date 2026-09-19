import test from 'node:test';
import assert from 'node:assert/strict';
import {validateForecast,forecastFor,learnedFallback,forecastAccuracy} from '../src/forecast.js';
import {analyzePlayer} from '../src/analysis.js';
import {healthyBaseline} from '../src/waiver-plan.js';
import {forecastEvidence,forecastResearch,forecastAudit} from '../public/forecast-ui.js';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {replacePlayerFeatures,getPlayerFeatureLookup} from '../src/db.js';

const now=Date.parse('2026-09-19T12:00:00Z');
const h={points:10,targets:6,carries:0,low:2,high:18,nominalCoverage:.8,measuredCoverage:.81,
  calibrationCount:1000,holdoutCount:1200,intervalTestCount:1300,approvedFallback:false,mae:3.1,baselineMae:3.2};
const f={version:'opportunity-v1:1234567890abcdef',season:2026,targetWeek:2,throughWeek:1,
  lastObservedWeek:1,lastObservedSeason:2026,historicalTeam:'SEA',historyGames:15,currentGames:1,
  horizons:{'1':h,'4':{...h,points:11,approvedFallback:true}},conditionalOnAppearance:true,scoringHash:'test'};
const player=()=>({playerId:'1',name:'Test WR',position:'WR',team:'SEA',proTeam:'SEA',injuryStatus:'ACTIVE',
  projectedPoints:12,seasonProjectedPoints:204,isAvailable:true,status:'FREEAGENT',onTeamId:0,
  game:{kickoff:'2026-09-20T17:00:00Z'},feature:{forecast:structuredClone(f),generatedAt:new Date(now).toISOString(),seasonPpg:8,games:15}});
const ctx={season:2026,currentWeek:2,now};

test('forecast validation rejects nonfinite, future, mismatched and oversized data',()=>{
  assert.equal(validateForecast(f,{season:2026,scoringHash:'test'}),f);
  for(const bad of [{...f,targetWeek:1},{...f,lastObservedWeek:2},{...f,conditionalOnAppearance:false},
    {...f,horizons:{'1':{...h,points:NaN}}},{...f,note:'x'.repeat(7000)}])assert.throws(()=>validateForecast(bad));
  assert.throws(()=>validateForecast(f,{scoringHash:'other'}));
});
test('stale, wrong-week or wrong-season forecasts are absent, not zero',()=>{
  const p=player();assert.ok(forecastFor(p,ctx));
  assert.equal(forecastFor(p,{...ctx,currentWeek:3}),null);
  assert.equal(forecastFor(p,{...ctx,season:2027}),null);
  assert.equal(forecastFor(p,{...ctx,now:now+37*3600000}),null);
  assert.equal(forecastFor(p,{}),null);
});
test('injury, team-change and long-gap contexts cannot promote forecasts',()=>{
  const p=player();
  assert.equal(forecastFor({...p,injuryStatus:'IR'},ctx).usable,false);
  assert.equal(forecastFor({...p,team:'NE'},ctx).usable,false);
  p.feature.forecast={...f,targetWeek:5,throughWeek:4};
  assert.equal(forecastFor(p,{...ctx,currentWeek:5}).usable,false);
});
test('historical NFL team aliases do not create a false team-change warning',()=>{
  const p=player();p.team=p.proTeam='LAR';p.feature.forecast.historicalTeam='LA';
  assert.equal(forecastFor(p,ctx).usable,true);
});
test('ESPN stays primary even with an approved learned forecast',()=>{
  const p=player();p.feature.forecast.horizons['1'].approvedFallback=true;
  const analysis=analyzePlayer(p,{...ctx,opponentLookup:{SEA:{opponent:'ARI',game:p.game}},featureLookup:{'1':p.feature}});
  assert.equal(analysis.median,12);assert.equal(analysis.estimateSource,'ESPN weekly projection');
  assert.equal(analysis.forecast.horizons['1'].points,10);
  assert.equal(healthyBaseline({...analysis,seasonProjectedPoints:204},3,2)>0,true);
  const unavailableProjection={...p,projectedPoints:null};
  assert.equal(analyzePlayer(unavailableProjection,{...ctx,opponentLookup:{SEA:{opponent:'ARI',game:p.game}},featureLookup:{'1':p.feature}}).median,10);
});
test('unqualified weekly model stays shadow and qualified horizon is not extrapolated',()=>{
  const p=player();p.forecast=forecastFor(p,ctx);
  assert.equal(learnedFallback(p),null);assert.equal(learnedFallback(p,4),11);
  const historyOnly={...p,projectedPoints:null,seasonProjectedPoints:null};
  assert.equal(healthyBaseline(historyOnly,3,2),11);
  assert.equal(healthyBaseline(historyOnly,6,2),8);
  assert.notEqual(healthyBaseline(p,3,2),11);
});
test('forecast accuracy uses matched pre-kickoff players without missing-score zeroes',()=>{
  const p=player();p.forecast=forecastFor(p,ctx);
  assert.equal(forecastAccuracy([p],{},2,now).count,0);
  assert.equal(forecastAccuracy([p],{'1':0},2,now).count,1);
  const a=forecastAccuracy([p,p],{'1':11},2,now);
  assert.equal(a.count,1);assert.equal(a.modelMae,1);assert.equal(a.espnMae,1);
  assert.equal(forecastAccuracy([p],{'1':11},2,now+2*86400000).count,0);
});
test('forecast UI distinguishes research from actionable moves and escapes text',()=>{
  const p=player();p.name='<script>oops</script>';p.forecast=forecastFor(p,ctx);
  const html=forecastEvidence(p);
  assert.match(html,/Research only/);assert.match(html,/not a guaranteed 80%/);
  assert.match(html,/per recorded appearance/);
  // No kickoff ensures this stable fixture is not silently removed after its date.
  const research=forecastResearch([{...p,game:null}]);
  assert.ok(!research.includes('<script>'));assert.match(research,/not an add\/drop ranking/);
  assert.match(forecastAudit({count:2,modelMae:3,espnMae:4,caveat:'Matched'}),/3.0 vs 4.0/);
});

test('forecast research balances positions and honors the selected position',()=>{
  const players=['QB','RB','WR','TE'].flatMap(position=>Array.from({length:10},(_,i)=>{
    const p={...player(),playerId:`${position}-${i}`,name:`${position} candidate ${i}`,position,game:null};
    p.forecast=forecastFor(p,ctx);
    p.forecast.horizons['4'].points=(position==='QB'?30:10)+i;
    return p;
  }));
  const all=forecastResearch(players);
  assert.equal((all.match(/<article/g)||[]).length,8);
  for(const position of ['QB','RB','WR','TE'])assert.match(all,new RegExp(`${position} candidate 9`));
  assert.ok(!all.includes('QB candidate 7'));
  const receivers=forecastResearch(players,'WR');
  assert.equal((receivers.match(/<article/g)||[]).length,8);
  assert.ok(!receivers.includes('QB candidate'));assert.ok(receivers.includes('WR candidate 2'));
  assert.match(forecastResearch(players,'K'),/Only QB, RB, WR and TE are modeled/);
});

test('forecast publications round-trip through real SQL and invalid batches keep active data',async t=>{
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());
  for(const name of ['0001_initial.sql','0002_decision_tracking.sql','0003_forecast_features.sql'])db.exec(readFileSync(new URL(`../migrations/${name}`,import.meta.url),'utf8'));
  const env={DB:{prepare(sql){const make=(args=[])=>({bind(...v){return make(v)},async first(){return db.prepare(sql).get(...args)||null},async all(){return{results:db.prepare(sql).all(...args)}},async run(){const result=db.prepare(sql).run(...args);return{meta:{changes:Number(result.changes)}}}});return make()},async batch(statements){const results=[];for(const s of statements)results.push(await s.run());return results}}};
  const payload={metadata:{leagueId:'1',season:2026,throughWeek:1,scoringHash:'test',generatedAt:new Date(now).toISOString()},rows:[{espnId:'1',position:'WR',playerName:'Fixture',forecast:f}]};
  await replacePlayerFeatures(env,payload);
  const lookup=await getPlayerFeatureLookup(env,'1',2026,['1']);
  assert.deepEqual(lookup['1'].forecast,f);assert.equal('forecastJson' in lookup['1'],false);
  const invalid={...payload,metadata:{...payload.metadata,generatedAt:new Date(now+1000).toISOString()},rows:[{...payload.rows[0],forecast:{...f,targetWeek:0}}]};
  await assert.rejects(()=>replacePlayerFeatures(env,invalid));
  assert.equal(db.prepare("SELECT COUNT(*) AS n FROM data_snapshots WHERE status='active'").get().n,1);
  assert.deepEqual((await getPlayerFeatureLookup(env,'1',2026,['1']))['1'].forecast,f);
});
