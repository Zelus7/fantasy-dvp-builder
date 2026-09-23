import test from 'node:test';
import assert from 'node:assert/strict';
import { allLeaguePlayers, buildOpponentLookup, discoverEspnLeagues, normalizeLeagueBundle, selectedOpponent, selectedTeam } from '../src/espn.js';

const stored={leagueId:'1269378',leagueName:'Test League',teamId:'1',teamName:'My Team',seasonYear:2026};
const body={id:1269378,seasonId:2026,scoringPeriodId:4,status:{currentMatchupPeriod:4},settings:{name:'Test League',rosterSettings:{rosterSize:3,lineupSlotCounts:{0:1,4:1,20:1}},scoringSettings:{scoringItems:[{statId:53,points:.5},{statId:3,points:.04}]},scheduleSettings:{matchupPeriodCount:14,playoffTeamCount:6}},teams:[{id:1,location:'Miami',nickname:'Managers',roster:{entries:[{lineupSlotId:4,playerPoolEntry:{player:{id:101,fullName:'Receiver One',defaultPositionId:4,eligibleSlots:[4,23],proTeamId:15,injuryStatus:'ACTIVE',stats:[{statSourceId:1,statSplitTypeId:1,seasonId:2026,scoringPeriodId:4,appliedTotal:12.4}]}}}]}},{id:2,location:'Other',nickname:'Team',roster:{entries:[{lineupSlotId:2,playerPoolEntry:{player:{id:202,fullName:'Runner Two',defaultPositionId:2,eligibleSlots:[2,23],proTeamId:2,injuryStatus:'QUESTIONABLE'}}}]}}],schedule:[{id:9,matchupPeriodId:4,home:{teamId:1,totalProjectedPoints:105},away:{teamId:2,totalProjectedPoints:100}}]};

test('league normalization preserves exact scoring and roster eligibility',()=>{const result=normalizeLeagueBundle(body,stored);assert.equal(result.league.scoringType,'half');assert.equal(result.league.currentWeek,4);assert.equal(result.teams[0].roster[0].projectedPoints,12.4);assert.deepEqual(result.teams[0].roster[0].eligibleSlotIds,[4,23]);assert.equal(result.currentMatchup.projectionMargin,5)});
test('selected team and opponent resolve from stored ESPN team id',()=>{const result=normalizeLeagueBundle(body,stored);assert.equal(selectedTeam(result).id,'1');assert.equal(selectedOpponent(result).id,'2')});
test('all league players retain fantasy-team ownership',()=>{const result=normalizeLeagueBundle(body,stored),players=allLeaguePlayers(result);assert.equal(players.length,2);assert.equal(players.find(player=>player.playerId==='202').fantasyTeamName,'Other Team')});
test('all opponents retain budget and tiebreak position; absent balances stay unknown',()=>{
  const raw=structuredClone(body);raw.settings.acquisitionSettings={isUsingAcquisitionBudget:true,acquisitionBudget:250,minimumBid:1};
  raw.teams[0].transactionCounter={acquisitionBudgetSpent:40};raw.teams[0].waiverRank=3;
  const result=normalizeLeagueBundle(raw,stored);assert.equal(result.teams[0].acquisition.remaining,210);assert.equal(result.teams[0].acquisition.waiverRank,3);assert.equal(result.teams[1].acquisition.remaining,null);
  raw.teams[1].transactionCounter={acquisitionBudgetSpent:''};assert.equal(normalizeLeagueBundle(raw,stored).teams[1].acquisition.remaining,null);
});
test('NFL opponent lookup is bidirectional',()=>{const game={eventId:'g',homeTeam:'MIA',awayTeam:'BUF'};const lookup=buildOpponentLookup([game]);assert.equal(lookup.MIA.opponent,'BUF');assert.equal(lookup.BUF.opponent,'MIA')});

const discoveryCredentials={swid:'test-owner',s2:'test-session'};
test('player position IDs are distinct from lineup slot IDs',()=>{
  for(const [id,position,slot] of [[1,'QB',0],[2,'RB',2],[3,'WR',4],[4,'TE',6],[5,'K',17],[16,'D/ST',16]]){
    const raw=structuredClone(body);
    const entry=raw.teams[0].roster.entries[0];
    entry.lineupSlotId=slot;
    entry.playerPoolEntry.player.defaultPositionId=id;
    const player=normalizeLeagueBundle(raw,stored).teams[0].roster[0];
    assert.equal(player.position,position);
  }
});
const discoveryOptions={leagueId:'1269378',seasonYear:2026};
const ownedLeague={id:1269378,seasonId:2026,settings:{name:'Test League'},teams:[{id:2,name:'Someone else',owners:['another-owner']},{id:9,name:'My Team',owners:['{TEST-OWNER}']}]};

test('Fan API 404 falls back to the known league and matches the cookie owner',async t=>{
  const calls=[];
  t.mock.method(globalThis,'fetch',async(url,options)=>{
    calls.push(String(url));
    if(calls.length===1)return new Response('Missing fan profile',{status:404});
    assert.equal(options.headers.Cookie,'SWID={test-owner}; espn_s2=test-session');
    return Response.json(ownedLeague);
  });
  const leagues=await discoverEspnLeagues(discoveryCredentials,discoveryOptions);
  assert.deepEqual(leagues,[{sport:'football',leagueId:'1269378',seasonYear:2026,leagueName:'Test League',teamId:'9',teamName:'My Team'}]);
  assert.equal(calls.length,2);
  assert.equal(calls[1],'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/2026/segments/0/leagues/1269378?view=mSettings&view=mTeam');
});

test('empty discovery results use the known league',async t=>{
  let count=0;
  t.mock.method(globalThis,'fetch',async()=>Response.json(++count===1?{preferences:[]}:ownedLeague));
  assert.equal((await discoverEspnLeagues(discoveryCredentials,discoveryOptions))[0].teamId,'9');
});

test('successful Fan API discovery preserves other leagues without fallback',async t=>{
  let count=0;
  t.mock.method(globalThis,'fetch',async()=>{
    count++;
    return Response.json({preferences:[{type:{code:'fantasy'},metaData:{entry:{gameId:1,entryId:4,seasonId:2026,groups:[{groupId:111,groupName:'Other league'}],entryMetadata:{teamName:'Another team'}}}}]});
  });
  assert.equal((await discoverEspnLeagues(discoveryCredentials,discoveryOptions))[0].leagueId,'111');
  assert.equal(count,1);
});

for(const responseStatus of [401,403])test(`discovery authentication error ${responseStatus} never falls back`,async t=>{
  let count=0;
  t.mock.method(globalThis,'fetch',async()=>{count++;return new Response(null,{status:responseStatus})});
  await assert.rejects(discoverEspnLeagues(discoveryCredentials,discoveryOptions),{status:502,code:'ESPN_AUTH_EXPIRED'});
  assert.equal(count,1);
});

for(const teams of [[{id:9,owners:['someone-else']}],[{id:9,owners:['test-owner']},{id:10,owners:['test-owner']}]])test('fallback refuses missing or ambiguous ownership',async t=>{
  let count=0;
  t.mock.method(globalThis,'fetch',async()=>++count===1?new Response(null,{status:404}):Response.json({...ownedLeague,teams}));
  await assert.rejects(discoverEspnLeagues(discoveryCredentials,discoveryOptions),{code:'ESPN_TEAM_OWNERSHIP_UNVERIFIED'});
});

test('fallback league authentication failures remain actionable',async t=>{
  let count=0;
  t.mock.method(globalThis,'fetch',async()=>new Response(null,{status:++count===1?404:401}));
  await assert.rejects(discoverEspnLeagues(discoveryCredentials,discoveryOptions),{code:'ESPN_AUTH_EXPIRED'});
});

test('fallback rejects a different season or league response',async t=>{
  let count=0;
  t.mock.method(globalThis,'fetch',async()=>++count===1?new Response(null,{status:404}):Response.json({...ownedLeague,seasonId:2025}));
  await assert.rejects(discoverEspnLeagues(discoveryCredentials,discoveryOptions),{code:'ESPN_INVALID_RESPONSE'});
});

test('explicit team selection works when ESPN omits owner identifiers',async t=>{
  let count=0;
  t.mock.method(globalThis,'fetch',async()=>++count===1?new Response(null,{status:404}):Response.json({...ownedLeague,teams:[{id:9,name:'My Team'}]}));
  const leagues=await discoverEspnLeagues(discoveryCredentials,{...discoveryOptions,teamId:'9'});
  assert.equal(leagues[0].teamId,'9');
  assert.equal(leagues[0].teamName,'My Team');
});

test('explicit team selection does not substitute a different available team',async t=>{
  let count=0;
  t.mock.method(globalThis,'fetch',async()=>++count===1?new Response(null,{status:404}):Response.json(ownedLeague));
  await assert.rejects(discoverEspnLeagues(discoveryCredentials,{...discoveryOptions,teamId:'99'}),{code:'ESPN_TEAM_OWNERSHIP_UNVERIFIED'});
});

test('explicit team selection cannot bypass ESPN league authorization',async t=>{
  let count=0;
  t.mock.method(globalThis,'fetch',async()=>new Response(null,{status:++count===1?404:403}));
  await assert.rejects(discoverEspnLeagues(discoveryCredentials,{...discoveryOptions,teamId:'9'}),{code:'ESPN_AUTH_EXPIRED'});
});
