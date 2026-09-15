import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import worker from '../src/index.js';
import { sha256, createSessionToken } from '../src/security.js';
import { getCredentials, listLeagues, cacheGet } from '../src/db.js';
import { currentNflSeason } from '../src/constants.js';

// Exercise the Worker route and actual schema; only D1 transport and ESPN are mocked.
function d1(db){
  return {
    prepare(sql){
      const make=(args=[])=>({
        bind(...values){return make(values)},
        async first(){return db.prepare(sql).get(...args)||null},
        async all(){return{results:db.prepare(sql).all(...args)}},
        async run(){const result=db.prepare(sql).run(...args);return{meta:{changes:Number(result.changes)}}}
      });
      return make();
    },
    async batch(statements){const results=[];for(const statement of statements)results.push(await statement.run());return results}
  };
}

test('paired sync survives Fan 404 and missing owners, then encrypts credentials and caches selected roster',async t=>{
  const db=new DatabaseSync(':memory:');
  t.after(()=>db.close());
  db.exec(readFileSync(new URL('../migrations/0001_initial.sql',import.meta.url),'utf8'));
  const token='test-device-token';
  db.prepare('INSERT INTO devices (id,token_hash,name) VALUES (?,?,?)').run('test-device',await sha256(token),'Test browser');
  const env={DB:d1(db),DEFAULT_LEAGUE_ID:'1269378',DEFAULT_TEAM_ID:'9',CREDENTIAL_ENCRYPTION_KEY:Buffer.alloc(32,1).toString('base64')};
  const season=currentNflSeason();
  const league={id:1269378,seasonId:season,scoringPeriodId:2,settings:{name:'Test league'},teams:[{id:9,name:'My Team',roster:{entries:[{lineupSlotId:0,playerPoolEntry:{player:{id:123,fullName:'Test Quarterback',defaultPositionId:1,eligibleSlots:[0,20],proTeamId:2}}}]}}]};
  const requests=[];
  t.mock.method(globalThis,'fetch',async url=>{
    requests.push(String(url));
    if(String(url).startsWith('https://fan.api.espn.com/'))return new Response(null,{status:404});
    assert.ok(String(url).startsWith(`https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/${season}/segments/0/leagues/1269378?`));
    return Response.json(league);
  });
  const request=new Request('https://app.example/api/extension/sync',{method:'POST',headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:JSON.stringify({cookies:{swid:'test-owner',s2:'test-session'}})});
  const response=await worker.fetch(request,env);
  const result=await response.json();
  assert.equal(response.status,200,JSON.stringify(result));
  assert.equal(result.connected,true);
  assert.equal(result.rosterCount,1);
  assert.equal(requests.filter(url=>url.includes('fan.api.espn.com')).length,1);
  assert.equal(requests.length,3);
  assert.equal((await getCredentials(env)).s2,'test-session');
  assert.equal((await listLeagues(env))[0].teamId,'9');
  const cache=await cacheGet(env,`espn:league:1269378:${season}`);
  assert.equal(cache.stale,false);
  assert.equal(cache.value.teams[0].roster[0].name,'Test Quarterback');
  const encrypted=db.prepare('SELECT encrypted_credentials FROM espn_credentials').get().encrypted_credentials;
  assert.equal(encrypted.includes('test-session'),false);
  // A failing public scoreboard must not discard the real synced roster or
  // imply that the user's private ESPN session has expired.
  t.mock.method(globalThis,'fetch',async url=>String(url).includes('/scoreboard')?new Response(null,{status:403}):Response.json(league));
  env.SESSION_SECRET='test-session-signing-key-at-least-32-characters';
  const session=await createSessionToken(env.SESSION_SECRET);
  const dashboard=await worker.fetch(new Request('https://app.example/api/dashboard',{headers:{Cookie:`fcc_session=${session}`}}),env);
  const dashboardBody=await dashboard.json();
  assert.equal(dashboard.status,200,JSON.stringify(dashboardBody));
  assert.equal(dashboardBody.team.name,'My Team');
  assert.equal(dashboardBody.roster.length,1);
  assert.equal(dashboardBody.roster[0].scheduleUnavailable,true);
  assert.equal(dashboardBody.roster[0].isAvailable,true);
  assert.equal(dashboardBody.roster[0].position,'QB');
  assert.equal(dashboardBody.summary.injuryAlerts,0);
  assert.ok(dashboardBody.summary.dataWarnings.some(w=>w.includes('schedule')));
  assert.equal(dashboardBody.dataHealth.espn.status,'connected');
});
