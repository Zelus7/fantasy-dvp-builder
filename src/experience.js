import {cacheGet,cachePut,getSettings,listLeagues,getDvpLookup,getNflSchedule,getPlayerFeatureLookup,getDataHealth,setSetting} from './db.js';
import {fetchLeagueBundle,fetchWaiverPool,fetchNflWeekSchedule,buildOpponentLookup,selectedTeam,selectedOpponent,allLeaguePlayers,fetchHistoricalPlayerScores,fetchWinningBids} from './espn.js';
import {analyzeRoster,optimizeLineup,bestAndToughestMatchups,buildScheduleOutlook,eligibleForSlot,adjustRiskWeights} from './analysis.js';
import {recommendWaiverMoves,discoverTradeTargets,evaluateBilateralTrade,byeCoverage,DECISION_METHOD} from './decisions.js';
import {json,HttpError,readJson} from './http.js';
import {buildWeatherLookup} from './weather.js';
import {fetchNews} from './news.js';
import {STARTER_SLOT_IDS} from './constants.js';
import {planWaivers,injuryHolds,WAIVER_METHOD} from './waiver-plan.js';
import {attachInjuryEvidence,validateInjuryEvidence} from './injury-evidence.js';
import {freezeDecision,decisionHistory} from './decision-store.js';
import {validateClaimPlan} from './claim-plan.js';
import {readClaimPlan,saveClaimPlan} from './claim-plan-store.js';

const nowIso=()=>new Date().toISOString();
const stamp=value=>value?Date.parse(String(value).includes('T')?value:String(value).replace(' ','T')+'Z'):NaN;
const uniqueIds=values=>[...new Set((Array.isArray(values)?values:[]).map(String).filter(v=>/^-?\d{1,12}$/.test(v)))].slice(0,100);
const preferenceKey=league=>`preferences:${league.leagueId}:${league.seasonYear}`;

export function sourceFreshness(updatedAt,maxAgeSeconds,details={}) {
  const age=Number.isFinite(stamp(updatedAt))?Math.max(0,(Date.now()-stamp(updatedAt))/1000):null;
  return {updatedAt:updatedAt||null,ageSeconds:age==null?null:Math.round(age),status:age==null?'missing':age>maxAgeSeconds?'stale':'fresh',...details};
}

async function resolve(env,opts) {
  const [leagues,settings]=await Promise.all([listLeagues(env),getSettings(env)]);
  const league=opts.leagueId?leagues.find(l=>String(l.leagueId)===String(opts.leagueId)&&(!opts.seasonYear||Number(l.seasonYear)===Number(opts.seasonYear))):leagues.find(l=>l.isDefault)||leagues[0];
  if(!league)throw new HttpError(409,'NO_CONNECTED_LEAGUE','The requested connected league is unavailable.');
  const preferences=settings[preferenceKey(league)]||{protectedIds:[],watchlistIds:[]};
  const bundle=await fetchLeagueBundle(env,league,{force:opts.force,week:opts.week});
  const team=selectedTeam(bundle);
  if(!team)throw new HttpError(502,'ESPN_TEAM_NOT_FOUND','The selected ESPN team is unavailable.');
  for(const t of bundle.teams)t.roster=attachInjuryEvidence(t.roster,league.seasonYear,settings[`injuries:${league.leagueId}:${league.seasonYear}`]||[]);
  return {league,settings,preferences,bundle,team,force:opts.force};
}

function parseOptions(url) {
  const raw=url.searchParams.get('week');
  if(raw!=null&&(!/^\d+$/.test(raw)||Number(raw)<1||Number(raw)>18))throw new HttpError(400,'INVALID_WEEK','Choose an NFL regular-season week from 1 to 18.');
  return {leagueId:url.searchParams.get('leagueId')||null,seasonYear:Number(url.searchParams.get('seasonYear'))||null,week:raw==null?null:Number(raw),force:url.searchParams.get('force')==='true'};
}
async function requestObject(request){const body=await readJson(request,8192);if(!body||typeof body!=='object'||Array.isArray(body))throw new HttpError(400,'INVALID_REQUEST','Send a JSON object.');return body}

async function context(env,state,players) {
  const {bundle,settings}=state,league=bundle.league,health=await getDataHealth(env,{leagueId:league.leagueId,season:league.seasonYear});
  let games=await getNflSchedule(env,league.seasonYear,{week:league.currentWeek}),scheduleError=null,scheduleFreshness=sourceFreshness(health.schedule?.generatedAt,24*3600);
  if(state.force||!games.length||scheduleFreshness.status!=='fresh')try{const live=await fetchNflWeekSchedule(env,league.seasonYear,league.currentWeek,{force:state.force});if(live.length){games=live;scheduleFreshness=sourceFreshness(nowIso(),24*3600)}}catch{scheduleError='Current schedule refresh failed; any retained schedule may have changed.'}
  const [dvpLookup,featureLookup]=await Promise.all([getDvpLookup(env,league.leagueId,league.seasonYear),getPlayerFeatureLookup(env,league.leagueId,league.seasonYear,players.map(p=>p.playerId))]);
  const espn=sourceFreshness(bundle.cache?.updatedAt,180,{status:bundle.cache?.stale?'stale':'fresh'});
  const through=health.playerFeatures?.metadata?.actualThroughWeek??health.playerFeatures?.throughWeek??null;
  const freshness={espn,statistics:sourceFreshness(health.playerFeatures?.generatedAt,36*3600,{throughWeek:through,coverage:'Historical production, not live scores. Prior-season blending is labeled in player evidence.'}),schedule:scheduleFreshness,injuries:{...espn,coverage:'ESPN roster designations only; practice reports and confirmed inactives are not a complete feed.'},news:{status:'missing',coverage:'No verified current news feed loaded.'}};
  freshness.dvp=sourceFreshness(health.dvp?.generatedAt,36*3600,{throughWeek:health.dvp?.metadata?.actualThroughWeek??health.dvp?.throughWeek??null});
  freshness.futureSchedule=sourceFreshness(health.schedule?.generatedAt,24*3600,{coverage:'Stored remaining-season schedule; separate from the selected-week fallback.'});
  if(freshness.dvp.throughWeek!=null&&freshness.dvp.throughWeek<Math.max(0,Number(league.liveWeek||league.currentWeek)-1))freshness.dvp.status='stale';
  if(through!=null&&through<Math.max(0,Number(league.liveWeek||league.currentWeek)-1))freshness.statistics.status='stale';
  const forecastMeta=health.playerFeatures?.metadata?.forecast;
  freshness.forecast=sourceFreshness(health.playerFeatures?.generatedAt,36*3600,{status:forecastMeta?.status==='ready'&&forecastMeta.targetWeek===league.currentWeek?freshness.statistics.status:'missing',coverage:forecastMeta?.status==='ready'?`${forecastMeta.rows} forecasts for week ${forecastMeta.targetWeek}. ${forecastMeta.version}. Conditional on a recorded appearance; not a complete injury forecast.`:'No matching independent forecasts published; ESPN and historical fallback remain available.'});
  const weatherLookup=await buildWeatherLookup(games,{get:async(key,stale=false)=>(await cacheGet(env,key,stale))?.value||null,put:(key,value,ttl)=>cachePut(env,key,value,ttl)});
  freshness.weather={status:Object.keys(weatherLookup).length?'partial':'missing',coverage:`Weather available for ${Object.keys(weatherLookup).length} of ${games.length} selected-week games. Roof flags reflect venue defaults; verify retractable-roof decisions.`};
  const news=await fetchNews(env,{force:state.force});if(news)freshness.news=sourceFreshness(news.generatedAt,3*3600,{coverage:'Source-linked ESPN headlines; not a complete injury feed.'});
  const historical=Number(league.currentWeek)<Number(league.liveWeek||league.currentWeek);
  const scheduleUnavailable=!games.length||games.some(g=>!Number.isFinite(stamp(g.kickoff)));
  const supportedSlots=Object.keys(league.lineupSlotCounts||{}).every(s=>[20,21,22].includes(Number(s))||STARTER_SLOT_IDS.has(Number(s)));
  const readinessReasons=[...(historical?['You selected a past week. Choose Current week for actionable advice; refreshing cannot change a historical selection.']:[]),...(bundle.cache?.stale?['The latest ESPN request failed; retained roster data is stale.']:[]),...(scheduleUnavailable||scheduleFreshness.status!=='fresh'?['The selected-week NFL schedule is missing or stale.']:[]),...(!supportedSlots?['This league uses starting slots the optimizer does not support.']:[])];
  return {health,games,season:league.seasonYear,currentWeek:league.currentWeek,now:Date.now(),scheduleUnavailable,scheduleWarning:scheduleError,historical,news:news?.items||[],freshness,readinessReasons,
    adviceReady:!bundle.cache?.stale&&!scheduleUnavailable&&scheduleFreshness.status==='fresh'&&!historical&&supportedSlots,
    opponentLookup:buildOpponentLookup(games),dvpLookup:freshness.dvp.status==='fresh'?dvpLookup:{},featureLookup:freshness.statistics.status==='fresh'?featureLookup:{},weatherLookup,
    riskWeights:adjustRiskWeights(settings.riskWeights,bundle.currentMatchup?.projectionMargin||0,settings.adaptiveRisk),
    unsupportedScoring:!!health.dvp?.metadata?.unsupportedScoring?.length};
}

async function outlook(env,state,players,ctx) {
  const league=state.bundle.league,start=league.currentWeek,end=Math.min(18,Math.max(...(state.settings.fantasyPlayoffWeeks||[15,16,17]),17));
  const games=ctx.freshness.futureSchedule.status==='fresh'?await getNflSchedule(env,league.seasonYear,{fromWeek:start,toWeek:end}):[],dvp=ctx.dvpLookup;
  const byWeek={};for(const game of games)(byWeek[game.week]||=[]).push(game);
  return buildScheduleOutlook(players,byWeek,dvp,start,end,state.settings.fantasyPlayoffWeeks);
}

function actionsFor(roster,lineup,ctx) {
  const actions=[];
  for(const p of roster.filter(p=>p.isStarter&&p.isAvailable===false&&(!p.game?.kickoff||stamp(p.game.kickoff)>Date.now())))actions.push({type:'urgent',title:`Check ${p.name}`,detail:`${p.injuryStatus||'Unavailable'}: review this starting slot before kickoff.`,playerId:p.playerId});
  for(const change of lineup.changes)actions.push({type:'lineup',title:change.start?`Start ${change.start.name}`:`Bench ${change.bench?.name||'the unavailable player'}`,detail:`${change.start&&change.bench?`Bench ${change.bench.name}. `:''}Follow the full recommended slot arrangement; FLEX moves may require a reshuffle. Verify availability.`,playerId:change.start?.playerId||change.bench?.playerId});
  if(!ctx.adviceReady)return [{type:'data',title:ctx.historical?'Choose Current week for advice':'Source check needed',detail:ctx.readinessReasons.join(' ')}];
  if(roster.length&&roster.every(p=>p.game?.kickoff&&stamp(p.game.kickoff)<=Date.now()))actions.push({type:'planning',title:'This week’s lineup is locked',detail:'All rostered players’ games have started. Choose the next week in the header for lineup/waiver planning, or use rest-of-season targets.'});
  return actions.slice(0,8);
}

function contingencies(roster) {
  return roster.filter(p=>p.isStarter&&['QUESTIONABLE','Q','DOUBTFUL'].includes(String(p.injuryStatus).toUpperCase())).map(p=>({player:p,
    alternatives:roster.filter(other=>!other.isStarter&&other.eligibleForRecommendation&&eligibleForSlot(other,p.lineupSlotId)&&other.game?.kickoff&&stamp(other.game.kickoff)>Date.now()).sort((a,b)=>b.decisionScore-a.decisionScore).slice(0,2)
      .map(other=>({player:other,decisionBy:stamp(other.game.kickoff)<stamp(p.game?.kickoff)?other.game.kickoff:p.game?.kickoff||other.game.kickoff,earlierKickoff:stamp(other.game.kickoff)<stamp(p.game?.kickoff)}))}));
}

async function recordVisit(env,state,roster,actions) {
  const key=`visits:${state.league.leagueId}:${state.league.seasonYear}`,previous=await cacheGet(env,key,true);
  const snapshot={at:nowIso(),week:state.bundle.league.currentWeek,players:roster.map(p=>({id:p.playerId,name:p.name,status:p.injuryStatus,projection:p.projectedPoints,actual:p.actualPoints})),actions:actions.map(a=>({type:a.type,title:a.title,detail:a.detail}))};
  const changes=[];
  if(previous&&previous.value.week===snapshot.week)for(const p of snapshot.players){const before=previous.value.players.find(v=>v.id===p.id);if(!before){changes.push(`${p.name} joined the roster.`);continue;}if(before.status!==p.status)changes.push(`${p.name}: ${before.status} → ${p.status}.`);if(before.projection!=null&&p.projection!=null&&Math.abs(p.projection-before.projection)>=1)changes.push(`${p.name}: ESPN projection ${before.projection.toFixed(1)} → ${p.projection.toFixed(1)}.`);}
  if(previous)for(const p of previous.value.players)if(!snapshot.players.some(v=>v.id===p.id))changes.push(`${p.name} is no longer on this roster.`);
  const historyKey=`history:${state.league.leagueId}:${state.league.seasonYear}`,history=(await cacheGet(env,historyKey,true))?.value||[];
  if(!previous||changes.length||previous.value.week!==snapshot.week||Date.now()-stamp(previous.value.at)>6*3600000){await cachePut(env,historyKey,[{at:snapshot.at,week:snapshot.week,changes,actions:snapshot.actions,method:DECISION_METHOD},...history].slice(0,30),90*86400);}
  await cachePut(env,key,snapshot,90*86400);
  return {changes,previousVisit:previous?.value?.at||null};
}

export async function buildWorkspace(env,opts={}) {
  const state=await resolve(env,opts),ctx=await context(env,state,state.team.roster),roster=analyzeRoster(state.team.roster,ctx),lineup=optimizeLineup(roster,state.bundle.league.lineupSlotCounts,state.team.roster);
  ctx.freshness.playerNews={status:'missing',updatedAt:null,coverage:'Credential-free browser player-news check pending. ESPN public news is unavailable from this Worker; roster loading does not wait for it.'};
  const schedules=await outlook(env,state,roster,ctx),actions=actionsFor(roster,lineup,ctx),visit=opts.recordVisit?await recordVisit(env,state,roster,actions):{};
  return {generatedAt:nowIso(),league:state.bundle.league,team:{id:state.team.id,name:state.team.name,record:state.team.record},opponent:selectedOpponent(state.bundle),fantasyMatchup:state.bundle.currentMatchup,
    roster,lineup,matchups:bestAndToughestMatchups(roster,6),schedules,depthPlan:byeCoverage(roster,state.bundle.league.lineupSlotCounts,schedules,{currentWeek:state.bundle.league.currentWeek}),actions,contingencies:ctx.adviceReady?contingencies(roster):[],preferences:state.preferences,riskWeights:ctx.riskWeights,
    freshness:ctx.freshness,readinessReasons:ctx.readinessReasons,adviceReady:ctx.adviceReady,historical:ctx.historical,news:ctx.news,cache:state.bundle.cache,dataHealth:ctx.health,...visit,
    summary:{injuryAlerts:roster.filter(p=>!['ACTIVE','NORMAL'].includes(String(p.injuryStatus).toUpperCase())).length,dataWarnings:[...(ctx.scheduleWarning?[ctx.scheduleWarning]:[]),...(ctx.scheduleUnavailable?['NFL schedule is unavailable.']:[]),...(ctx.unsupportedScoring?['Historical NFL statistics cannot reproduce every league scoring rule; ESPN actual scores remain authoritative.']:[]),...(ctx.historical?['Past-week scores belong to the current roster, not a reconstructed historical lineup.']:[])]}};
}

export async function buildOpportunities(env,opts={}) {
  const state=await resolve(env,opts),mode=opts.mode||'week';if(!['week','bridge','ros'].includes(mode))throw new HttpError(400,'INVALID_HORIZON','Choose this week, four-week bridge or rest of season.');
  const free=await fetchWaiverPool(env,state.league,opts.position||null,{force:opts.force,week:state.bundle.league.currentWeek});
  const pool=[...allLeaguePlayers(state.bundle),...free.players],ctx=await context(env,state,pool),analyzed=analyzeRoster(pool,ctx),byId=new Map(analyzed.map(p=>[String(p.playerId),p]));
  const roster=state.team.roster.map(p=>byId.get(String(p.playerId))),agents=free.players.map(p=>byId.get(String(p.playerId))),schedules=await outlook(env,state,pool,ctx);
  const settings={mode,slots:state.bundle.league.lineupSlotCounts,schedules,protectedIds:state.preferences.protectedIds||[],rosterCapacity:state.bundle.league.rosterSize,limit:24,currentWeek:state.bundle.league.currentWeek,endWeek:Math.min(18,Math.max(...(state.settings.fantasyPlayoffWeeks||[15,16,17]))),now:Date.now(),market:{...state.bundle.league.acquisition,...state.team.acquisition,verifiedAt:state.bundle.cache?.stale?null:state.bundle.cache?.updatedAt}};
  const teams=state.bundle.teams.map(team=>({...team,roster:team.roster.map(p=>byId.get(String(p.playerId)))}));
  const market=await fetchWinningBids(env,state.league,pool,{force:opts.force});Object.assign(settings.market,market,{yourTeamId:state.team.id,slots:settings.slots,teams:teams.map(({id,name,roster,acquisition})=>({id,name,roster,acquisition}))});
  const recommendations=!opts.clientCompute&&ctx.adviceReady&&!free.cache?.stale?planWaivers(agents,roster,settings):[];
  const trades=!opts.clientCompute&&opts.includeTrades&&ctx.adviceReady?discoverTradeTargets(teams,state.team.id,settings):[];
  const calculationInput={roster,freeAgents:agents,teams:opts.includeTrades?teams.map(t=>({id:t.id,name:t.name,roster:t.roster})):[],yourTeamId:state.team.id,settings,position:opts.position,includeTrades:opts.includeTrades,adviceReady:ctx.adviceReady,waiversReady:ctx.adviceReady&&!free.cache?.stale};
  let snapshot=null,snapshotWarning=null;try{snapshot=await freezeDecision(env,state.bundle.league,calculationInput,ctx.freshness)}catch{snapshotWarning='This decision could not be archived; it will not count toward model validation.'}
  return {generatedAt:nowIso(),method:WAIVER_METHOD,snapshot,snapshotWarning,market:{...settings.market,teams:undefined},league:state.bundle.league,mode,recommendations,trades,holds:!opts.clientCompute&&calculationInput.waiversReady?injuryHolds(roster,agents,settings):[],adviceReady:calculationInput.waiversReady,freshness:{...ctx.freshness,waivers:sourceFreshness(free.cache?.updatedAt,300,{status:free.cache?.stale?'stale':'fresh',coverage:free.coverage})},
    teams:opts.includeTrades?teams.map(t=>({id:t.id,name:t.name,roster:t.roster})):undefined,
    calculationInput:opts.clientCompute?calculationInput:undefined,
    watchlist:analyzed.filter(p=>(state.preferences.watchlistIds||[]).includes(String(p.playerId))),
    explanation:'Compare each move with keeping your roster and starting its best legal lineup. Future weeks use baseline scenarios, not calibrated forecasts. The best 48 screened add/drop pairs receive detailed multiweek comparisons. No bench-weight bonus is added.',
    emptyReason:!ctx.adviceReady?ctx.readinessReasons.join(' '):free.cache?.stale?'The waiver refresh failed; retry before acting.':'No moves passed the legal-lineup and value checks, or future schedule coverage is incomplete. Keeping your roster is a valid result.'};
}

export async function handleExperience(request,env) {
  const url=new URL(request.url),path=({'/api/dashboard':'/api/workspace','/api/waivers':'/api/opportunities','/api/trade':'/api/trade-review'})[url.pathname]||url.pathname;
  if(!['/api/workspace','/api/opportunities','/api/trade-review','/api/preferences','/api/history','/api/recommendation-history','/api/injury-evidence','/api/decision-history','/api/decision-replay','/api/decision-feedback','/api/claim-plan'].includes(path))return null;
  const opts=parseOptions(url);
  if(path==='/api/claim-plan'&&['GET','PUT'].includes(request.method)){
    if(request.method==='PUT'&&request.headers.get('origin')&&request.headers.get('origin')!==url.origin)throw new HttpError(403,'ORIGIN_DENIED','Save this plan from the command center.');
    const force=request.method==='PUT'||opts.force,state=await resolve(env,{...opts,force});
    const league=state.bundle.league,plan=await readClaimPlan(env,league);
    const [free,storedGames,health]=await Promise.all([fetchWaiverPool(env,state.league,null,{force,week:league.currentWeek}),getNflSchedule(env,league.seasonYear,{week:league.currentWeek}),getDataHealth(env,{leagueId:league.leagueId,season:league.seasonYear})]);
    let games=storedGames,scheduleFreshness=sourceFreshness(health.schedule?.generatedAt,24*3600),scheduleWarnings=[];
    if(force||!games.length||scheduleFreshness.status!=='fresh')try{const live=await fetchNflWeekSchedule(env,league.seasonYear,league.currentWeek,{force});if(live.length){games=live;scheduleFreshness=sourceFreshness(nowIso(),24*3600);}}catch{scheduleWarnings.push('Direct scoreboard refresh unavailable. Using the last verified schedule only if it is less than 24 hours old; confirm final game locks in ESPN.');}
    const market={...league.acquisition,...state.team.acquisition},kickoffs=Object.fromEntries(games.flatMap(g=>[[g.homeTeam,g.kickoff],[g.awayTeam,g.kickoff]]));
    const context={market,roster:state.team.roster,freeAgents:free.players,kickoffs,week:league.currentWeek,liveWeek:league.liveWeek,rosterCapacity:league.rosterSize,protectedIds:state.preferences.protectedIds||[],stale:state.bundle.cache?.stale||free.cache.stale,scheduleReady:games.length>0&&scheduleFreshness.status==='fresh',warnings:scheduleWarnings,verifiedAt:[state.bundle.cache?.updatedAt,free.cache.updatedAt].filter(Boolean).sort()[0]};
    let saved=plan,review;
    if(request.method==='PUT'){
      const body=await requestObject(request);review=validateClaimPlan(body,context);
      if(!review.valid)throw new HttpError(409,'PLAN_NOT_READY',review.errors.join(' '));
      saved=await saveClaimPlan(env,league,body.version,review);
    }else review=validateClaimPlan({...plan,week:plan.week??league.currentWeek},context);
    const player=p=>({playerId:p.playerId,name:p.name,position:p.position,injuryStatus:p.injuryStatus,status:p.status});
    return json({plan:saved,review,context:{week:league.currentWeek,liveWeek:league.liveWeek,market,freeAgents:free.players.map(player),roster:state.team.roster.map(player),coverage:free.coverage},
      espnUrl:`https://fantasy.espn.com/football/team?leagueId=${encodeURIComponent(league.leagueId)}&teamId=${encodeURIComponent(league.teamId)}&seasonId=${league.seasonYear}`,
      submissionEnabled:false,explanation:'Saved review worksheet only. The app does not submit, edit or cancel ESPN claims. Reported pending claims are user-reported, not ESPN-verified receipts.'});
  }
  if(path==='/api/decision-feedback'&&request.method==='POST'){
    const state=await resolve(env,opts),body=await requestObject(request),row=await env.DB.prepare('SELECT inputs_json AS inputsJson FROM decision_snapshots WHERE id=? AND league_id=? AND season=?').bind(String(body.id),String(state.league.leagueId),state.league.seasonYear).first();
    if(!row)throw new HttpError(404,'SNAPSHOT_NOT_FOUND','Decision snapshot not found.');
    const inputs=JSON.parse(row.inputsJson),cost=body.cost===''||body.cost==null?null:Number(body.cost);
    if(!['acquired','missed','skipped'].includes(body.status)||body.status==='acquired'&&!body.playerId||cost!=null&&(!Number.isInteger(cost)||cost<0||cost>100000)||body.playerId&&!inputs.freeAgents.some(p=>p.playerId===String(body.playerId)))throw new HttpError(400,'INVALID_FEEDBACK','Choose a valid claim outcome, player and nonnegative whole-dollar cost.');
    const feedback={status:body.status,playerId:body.playerId||null,cost:body.status==='acquired'?cost:null,at:nowIso(),source:'User reported; not independently verified in ESPN'};
    await env.DB.prepare('UPDATE decision_snapshots SET result_json=? WHERE id=?').bind(JSON.stringify(feedback),String(body.id)).run();return json({saved:true});
  }
  if(path==='/api/decision-history'&&request.method==='GET'){const state=await resolve(env,opts);return json({entries:await decisionHistory(env,state.league)});}
  if(path==='/api/decision-replay'&&request.method==='GET'){
    const state=await resolve(env,opts),row=await env.DB.prepare('SELECT method,inputs_json AS inputsJson,week FROM decision_snapshots WHERE id=? AND league_id=? AND season=?').bind(url.searchParams.get('id')||'',String(state.league.leagueId),state.league.seasonYear).first();
    if(!row)throw new HttpError(404,'SNAPSHOT_NOT_FOUND','Decision snapshot not found.');
    const inputs=JSON.parse(row.inputsJson),players=[...inputs.roster,...inputs.freeAgents];
    const completedWeek=row.week<state.bundle.league.liveWeek,actuals=completedWeek?await fetchHistoricalPlayerScores(env,state.league,row.week,players.map(p=>p.playerId)):{};
    return json({snapshot:{method:row.method,inputs},actuals,completedWeek:row.week<state.bundle.league.liveWeek,explanation:'Exact-week ESPN actual scores. Missing scores are not assumed to be zero. Recent results remain provisional until ESPN stat corrections settle.'});
  }
  if(path==='/api/injury-evidence'&&request.method==='PUT'){
    const state=await resolve(env,opts),body=await requestObject(request);let evidence;
    try{evidence=validateInjuryEvidence(body,state.league.seasonYear)}catch(error){throw new HttpError(400,'INVALID_EVIDENCE',error.message)}
    if(!state.team.roster.some(p=>p.playerId===evidence.playerId))throw new HttpError(400,'PLAYER_NOT_ROSTERED','Choose a rostered player.');
    const key=`injuries:${state.league.leagueId}:${state.league.seasonYear}`,existing=state.settings[key]||[];
    await setSetting(env,key,[...existing.filter(e=>e.playerId!==evidence.playerId),evidence].slice(-30));return json(evidence);
  }
  if(path==='/api/recommendation-history'&&request.method==='POST'){
    const body=await requestObject(request),state=await resolve(env,opts);
    if(![DECISION_METHOD,WAIVER_METHOD].includes(body.method)||!Array.isArray(body.actions))throw new HttpError(400,'INVALID_HISTORY','A known model and action list are required.');
    const actions=body.actions.slice(0,6).filter(a=>a&&['waiver','trade'].includes(a.type)&&typeof a.title==='string').map(a=>({type:a.type,title:a.title.slice(0,200),detail:String(a.detail||'').slice(0,600)}));
    if(!actions.length)return json({recorded:false});
    const key=`history:${state.league.leagueId}:${state.league.seasonYear}`,history=(await cacheGet(env,key,true))?.value||[],mode=body.mode==='ros'?'ros':'week',week=state.bundle.league.currentWeek;
    if(history.some(h=>h.kind==='recommendations'&&h.week===week&&h.mode===mode&&JSON.stringify(h.actions)===JSON.stringify(actions)&&Date.now()-stamp(h.at)<6*3600000))return json({recorded:false});
    await cachePut(env,key,[{at:nowIso(),kind:'recommendations',week,mode,actions,changes:[],method:body.method,source:'Browser model snapshot, not executed transactions'},...history].slice(0,30),90*86400);return json({recorded:true});
  }
  if(path==='/api/workspace'&&request.method==='GET')return json(await buildWorkspace(env,{...opts,recordVisit:url.searchParams.get('visit')==='true'}));
  if(path==='/api/opportunities'&&request.method==='GET')return json(await buildOpportunities(env,{...opts,mode:url.searchParams.get('mode')||'week',position:url.searchParams.get('position')||null,includeTrades:url.searchParams.get('trades')==='true',clientCompute:url.searchParams.get('compute')==='browser'}));
  if(path==='/api/history'&&request.method==='GET'){const state=await resolve(env,opts);return json({entries:(await cacheGet(env,`history:${state.league.leagueId}:${state.league.seasonYear}`,true))?.value||[]});}
  if(path==='/api/preferences'&&request.method==='PUT'){const body=await requestObject(request),state=await resolve(env,opts),value={protectedIds:uniqueIds(body.protectedIds),watchlistIds:uniqueIds(body.watchlistIds)};await setSetting(env,preferenceKey(state.league),value);return json(value);}
  if(path==='/api/trade-review'&&request.method==='POST'){
    const body=await requestObject(request),state=await resolve(env,opts),pool=allLeaguePlayers(state.bundle),ctx=await context(env,state,pool),players=analyzeRoster(pool,ctx),byId=new Map(players.map(p=>[String(p.playerId),p]));
    const theirs=state.bundle.teams.find(t=>String(t.id)===String(body.otherTeamId)&&String(t.id)!==String(state.team.id));
    if(!theirs)throw new HttpError(400,'TRADE_TEAM_REQUIRED','Choose another league team.');
    if(!ctx.adviceReady)throw new HttpError(409,'DATA_NOT_READY','Refresh ESPN and schedule data before evaluating a trade.');
    return json(evaluateBilateralTrade(state.team.roster.map(p=>byId.get(String(p.playerId))),theirs.roster.map(p=>byId.get(String(p.playerId))),body.giveIds||[],body.receiveIds||[],{slots:state.bundle.league.lineupSlotCounts,schedules:await outlook(env,state,pool,ctx),protectedIds:state.preferences.protectedIds||[]}));
  }
  throw new HttpError(405,'METHOD_NOT_ALLOWED','This action is not supported.');
}
