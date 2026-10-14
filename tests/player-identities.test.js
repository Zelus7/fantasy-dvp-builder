import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {fetchPublicIdentity,mergeIdentities,resolvePlayerIdentities} from '../src/player-identities.js';
import {enrichActivity} from '../src/waiver-activity.js';

const league={leagueId:'1',seasonYear:2026};
function database(t){
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());
  db.exec(readFileSync(new URL('../migrations/0001_initial.sql',import.meta.url),'utf8'));
  return {db,env:{DB:{prepare(sql){const make=(a=[])=>({bind(...v){return make(v)},async first(){return db.prepare(sql).get(...a)},async all(){return {results:db.prepare(sql).all(...a)}},async run(){return db.prepare(sql).run(...a)}});return make()}}}};
}

test('identity merge retains position and normalizes numeric IDs without preserving roster fields',()=>{
  const players=mergeIdentities([{playerId:21,name:'Target',position:'RB',isAvailable:true}], [{playerId:'21',name:'Target'}, {playerId:'22',name:'Player 22'}]);
  assert.deepEqual(players,[{playerId:'21',name:'Target',position:'RB'}]);
  const [row]=enrichActivity([{playerId:21,dropId:22,teamId:9}],players,[{id:'9',name:'My team'}]);
  assert.equal(row.playerName,'Target');assert.equal(row.position,'RB');assert.equal(row.teamName,'My team');assert.equal(row.dropName,'Player name unavailable');
});

test('public lookup uses fixed origins, GET and no credentials; defense IDs resolve correctly',async t=>{
  const urls=[];
  t.mock.method(globalThis,'fetch',async(url,options)=>{
    urls.push(url);assert.equal(options.method,'GET');assert.equal(options.redirect,'manual');assert.deepEqual(options.headers,{Accept:'application/json'});
    return Response.json(url.endsWith('/12')?{team:{id:'12',name:'Chiefs'}}:{id:'4429059',displayName:'Woody Marks',position:{abbreviation:'RB'}});
  });
  assert.equal((await fetchPublicIdentity('4429059')).name,'Woody Marks');
  assert.deepEqual(await fetchPublicIdentity(-16012),{playerId:'-16012',name:'Chiefs D/ST',position:'D/ST'});
  for(const invalid of ['https://evil.example',0,-1,'12/../4',-16100])assert.equal(await fetchPublicIdentity(invalid),null);
  assert.equal(urls.length,2);assert.ok(urls.every(url=>new URL(url).hostname.endsWith('.espn.com')));
});

test('mismatched, redirected, invalid and oversized identity responses fail without inventing names',async t=>{
  for(const response of [Response.json({id:99,displayName:'Wrong player'}),Response.json({id:21,displayName:'<script>bad</script>'}),new Response(null,{status:302,headers:{location:'https://evil.example'}}),new Response('x'.repeat(128*1024+1)),new Response('not json')]){
    t.mock.method(globalThis,'fetch',async()=>response);assert.equal(await fetchPublicIdentity('21'),null);t.mock.restoreAll();
  }
  t.mock.method(globalThis,'fetch',async()=>{throw new Error('timeout')});assert.equal(await fetchPublicIdentity(21),null);
});

test('names survive roster departures, cache expiry and lookup outages without changing freshness',async t=>{
  const {db,env}=database(t);let calls=0;
  t.mock.method(globalThis,'fetch',async()=>{calls++;return Response.json({id:'21',displayName:'Direct ESPN claim',position:{abbreviation:'RB'}})});
  const first=await resolvePlayerIdentities(env,league,[{playerId:10,name:'Roster drop',position:'QB',status:'FREEAGENT'}],['21','10']);
  assert.equal(first.coverage.unresolved,0);assert.equal(calls,1);
  db.exec("UPDATE api_cache SET expires_at='2020-01-01'");
  const second=await resolvePlayerIdentities(env,league,[],['21','10']);
  assert.equal(second.players.find(p=>p.playerId==='21').name,'Direct ESPN claim');assert.equal(calls,1);assert.equal('status' in second.players[0],false);
  await resolvePlayerIdentities(env,league,[{playerId:22,name:'Later player'}],[]);
  assert.equal((await resolvePlayerIdentities(env,league,[],['10','21','22'])).coverage.unresolved,0);
  const other=await resolvePlayerIdentities(env,{...league,leagueId:'2'},[],['10']);assert.equal(other.coverage.unresolved,1,'another league cannot inherit the retained identity');
});

test('lookups are bounded and failure to retain identities does not erase available names',async t=>{
  const {env}=database(t);let calls=0,active=0,max=0;
  t.mock.method(globalThis,'fetch',async url=>{calls++;active++;max=Math.max(max,active);await Promise.resolve();active--;return Response.json({id:url.split('/').at(-1),displayName:'Fixture target'})});
  const result=await resolvePlayerIdentities(env,league,[],Array.from({length:30},(_,i)=>String(i+1)));
  assert.equal(calls,12);assert.ok(max<=6);assert.equal(result.coverage.unresolved,18);
  const failing={DB:{prepare(){throw new Error('storage unavailable')}}};
  const fallback=await resolvePlayerIdentities(failing,league,[{playerId:21,name:'Known player'}],['21']);
  assert.equal(fallback.players[0].name,'Known player');assert.equal(fallback.coverage.persistence,'unavailable');
});

test('a dropped player is named from scoped normalized waiver caches even if public lookup fails',async t=>{
  const {db,env}=database(t);let calls=0;
  t.mock.method(globalThis,'fetch',async()=>{calls++;throw new Error('Public lookup unavailable from worker')});
  const insert=db.prepare('INSERT INTO api_cache(cache_key,payload_json,expires_at,updated_at) VALUES(?,?,?,?)');
  insert.run('espn:v2:freeagents:1:2026:6:QB:75',JSON.stringify([{playerId:'4426338',name:'Bo Nix',position:'QB',status:'FREEAGENT',projectedPoints:18}]),'2020-01-01','2026-10-14 12:00:00');
  insert.run('espn:v2:freeagents:2:2026:6:QB:75',JSON.stringify([{playerId:'4426338',name:'Wrong league name',position:'QB'}]),'2099-01-01','2026-10-14 13:00:00');
  const result=await resolvePlayerIdentities(env,league,[],['4426338']);
  assert.deepEqual(result.players,[{playerId:'4426338',name:'Bo Nix',position:'QB'}]);assert.equal(calls,0);assert.equal(result.coverage.unresolved,0);
  db.prepare('DELETE FROM api_cache WHERE cache_key LIKE ?').run('espn:v2:freeagents:%');
  assert.equal((await resolvePlayerIdentities(env,league,[],['4426338'])).players[0].name,'Bo Nix');assert.equal(calls,0);
});
