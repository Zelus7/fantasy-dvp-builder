// Synthetic UI fixture only. Never deployed and never accesses ESPN credentials.
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {resolve,extname} from 'node:path';
import {analyzeRoster,optimizeLineup,bestAndToughestMatchups} from '../src/analysis.js';
import {planWaivers as recommendWaiverMoves,injuryHolds,WAIVER_METHOD} from '../src/waiver-plan.js';
import {discoverTradePlans,evaluateTradePlan} from '../src/trade-plan.js';
import {emergingWaiverTargets} from '../src/waiver-signals.js';
import {withSecurityHeaders} from '../src/http.js';
import {validateClaimPlan} from '../src/claim-plan.js';
import {reviewOfferReport,reconcileClaimPlan} from '../src/claim-outcomes.js';
import {readSnapshot} from '../src/snapshot-codec.js';
import {waiverReviewCoverage} from '../src/waiver-review.js';
const reviewFixture=process.env.FCC_PREVIEW_REVIEW==='1';
const stamp=new Date().toISOString();
const roleFeature=points=>({season:2026,generatedAt:stamp,dataThroughWeek:4,currentGames:4,seasonPpg:10,opportunity:{games:4,throughWeek:4,weeklyPoints:points.map((points,i)=>({week:i+1,points})),weeklyUsage:points.map((_,i)=>({week:i+1,team:'BUF',carries:11,targets:1}))}});
const kickingFeature=attempts=>({season:2026,generatedAt:stamp,dataThroughWeek:4,opportunity:{contextOnly:true,kicking:{season:2026,team:'BUF',generatedAt:stamp,throughWeek:4,games:4,attempts:attempts.reduce((a,b)=>a+b,0),made:attempts.reduce((a,b)=>a+b,0),patMade:8,patAttempts:8,longMade:2,longAttempts:2,weekly:attempts.map((attempts,i)=>({week:i+1,attempts})),source:'Synthetic fixture'}}});
const snapshotFile=process.env.FCC_PREVIEW_SNAPSHOT;
const snapshotRow=snapshotFile?JSON.parse(await readFile(snapshotFile,'utf8'))[0].results[0]:null;
const snapshot=snapshotRow?await readSnapshot(snapshotRow.inputs_json,snapshotRow.input_hash):null;
const settings={riskWeights:{floor:.2,median:.6,ceiling:.2},adaptiveRisk:false,fantasyPlayoffWeeks:[15,16,17]};
let preferences={protectedIds:[],watchlistIds:[]};
const slots=snapshot?.settings.slots||(reviewFixture?{0:1,2:1,4:1,17:1,20:3}:{2:1,4:1,20:2});
const make=(id,name,position,value,extra={})=>({playerId:String(id),name,position,projectedPoints:value,seasonProjectedPoints:value*17,actualPoints:id===1?0:id===2?-2.2:null,eligibleSlotIds:[position==='RB'?2:4,23],proTeam:'BUF',injuryStatus:'ACTIVE',isStarter:id===1||id===3,lineupSlotId:position==='RB'?2:4,...extra});
const context={riskWeights:settings.riskWeights,opponentLookup:{BUF:{opponent:'MIA',game:{kickoff:'2099-09-12T17:00:00Z',eventId:'fixture'}}},dvpLookup:{'WR:MIA':{percentile:80,rank:7,grade:'A',confidence:.4},'RB:MIA':{percentile:30,rank:22,grade:'D',confidence:.4}}};
const mine=snapshot?.roster||analyzeRoster([make(1,'Fixture Lead Runner','RB',20),make(2,'Fixture Bench Runner','RB',15),make(3,'Fixture Receiver','WR',5),make(4,'Fixture Injured Receiver','WR',18,{injuryStatus:'OUT',percentOwned:99,isStarter:false,lineupSlotId:20,injuryEvidence:{earliestWeek:9,earlyWeek:9,planningWeek:10,lateWeek:12,reviewBy:'2099-01-01T00:00:00Z'}})],context);
const theirs=analyzeRoster([make(11,'Fixture Lead Receiver','WR',20),make(12,'Fixture Bench Receiver','WR',15),make(13,'Fixture Other Runner','RB',5)],context);
const agents=snapshot?.freeAgents||analyzeRoster([make(21,'Fixture Waiver Receiver','WR',12,{status:'FREEAGENT'}),make(22,'Fixture Waiver Runner','RB',8,{status:'WAIVERS'})],context);
if(reviewFixture&&!snapshot){
  mine.push(...analyzeRoster([make(31,'Fixture Hold Kicker','K',8.8,{eligibleSlotIds:[17],lineupSlotId:17,feature:kickingFeature([2,1,1,1])}),make(32,'Fixture Starting QB','QB',22,{eligibleSlotIds:[0],lineupSlotId:0}),make(33,'Fixture Spare QB','QB',18,{eligibleSlotIds:[0],lineupSlotId:20})],context));
  agents.push(...analyzeRoster([make(34,'Fixture Opportunity Kicker','K',8.9,{eligibleSlotIds:[17],lineupSlotId:20,status:'WAIVERS',feature:kickingFeature([1,3,4,3])})],context));
  mine.find(p=>p.playerId==='31').feature=kickingFeature([2,1,1,1]);agents.find(p=>p.playerId==='34').feature=kickingFeature([1,3,4,3]);
  agents[0].feature=roleFeature([13,14,12,11]);agents[0].status='WAIVERS';
  agents[1].feature=roleFeature([3.6,3.6,5.8,6.2]);agents[1].projectedPoints=10.1;agents[1].median=10.1;
}
const planSettings=()=>snapshot?{...snapshot.settings,now:Date.now()}:({season:2026,rolePlayers:[...mine,...theirs,...agents],slots,rosterCapacity:mine.length,currentWeek:5,endWeek:17,now:Date.now(),market:{type:'FAAB',budget:250,remaining:250,minimumBid:1,verifiedAt:new Date().toISOString()},schedules:Object.fromEntries([...mine,...theirs,...agents].map(p=>[p.playerId,{weeks:Array.from({length:17},(_,i)=>({week:i+1,bye:false,missingSchedule:false}))}]))});
const teams=[{id:'9',name:'Synthetic Preview Team',roster:mine},{id:'2',name:'Synthetic Other Team',roster:theirs}];
const league={leagueId:'fixture',teamId:'9',seasonYear:2026,leagueName:snapshot?'LOCAL SNAPSHOT — NOT A LIVE CONNECTION':'SYNTHETIC TEST DATA',currentWeek:snapshot?.settings.currentWeek||5,liveWeek:snapshot?.settings.currentWeek||5,lineupSlotCounts:slots,rosterSize:mine.length,isDefault:true};
const sources=()=>({espn:{status:'fresh',updatedAt:new Date().toISOString()},statistics:{status:'stale',updatedAt:'2026-01-01T00:00:00Z',throughWeek:0,coverage:'Synthetic fixture, not your live data'},news:{status:'missing',coverage:'Fixture has no news feed'}});
const workspace=week=>({generatedAt:new Date().toISOString(),league:{...league,currentWeek:week||league.currentWeek},team:teams[0],opponent:teams[1],roster:mine,lineup:optimizeLineup(mine,slots,mine),matchups:bestAndToughestMatchups(mine),schedules:{},actions:[{type:'lineup',title:'Review your WR depth',detail:'Synthetic fixture action'}],contingencies:[],preferences,riskWeights:settings.riskWeights,freshness:sources(),adviceReady:true,news:[],summary:{injuryAlerts:1,dataWarnings:[snapshot?'PRIVATE LOCAL SNAPSHOT — not a live ESPN connection':'SYNTHETIC LOCAL PREVIEW — not live ESPN data']},changes:['Fixture roster change'],historical:false});
const root=resolve('public');
let claimPlan={version:0,week:league.currentWeek,claims:[],spendingLimit:0,updatedAt:null};
const claimHistory=[];
const server=http.createServer(async(req,res)=>{
  try{
    const url=new URL(req.url,'http://127.0.0.1'),path=url.pathname;
    let body={};if(req.method!=='GET'){let raw='';for await(const chunk of req)raw+=chunk;body=JSON.parse(raw||'{}')}
    let data;
    if(path==='/api/auth/session')data={authenticated:true};
    else if(path==='/api/leagues')data={leagues:[league],settings};
    else if(path==='/api/workspace')data=workspace(Number(url.searchParams.get('week')));
    else if(path==='/api/opportunities'){
      const mode=url.searchParams.get('mode')||'week',position=url.searchParams.get('position');
      const filtered=agents.filter(p=>!position||p.position===position),options={mode,...planSettings(),...preferences};
      data={recommendations:recommendWaiverMoves(filtered,mine,options),trades:discoverTradePlans(teams,'9',options),emerging:emergingWaiverTargets(filtered,mine,options),tradeFreeAgents:agents,teams,holds:injuryHolds(mine,agents,options),watchlist:[...mine,...agents].filter(p=>preferences.watchlistIds.includes(p.playerId)),adviceReady:true,explanation:'Synthetic calculation fixture',emptyReason:'No fixture moves pass the filters',freshness:sources()};
      data.reviewCoverage=waiverReviewCoverage(filtered,data.recommendations);
      if(url.searchParams.get('compute')==='browser')data.calculationInput={roster:mine,freeAgents:agents.filter(p=>!position||p.position===position),teams,yourTeamId:'9',settings:{mode,...planSettings(),...preferences},includeTrades:url.searchParams.get('trades')==='true',adviceReady:true,waiversReady:true};
    }else if(path==='/api/preferences'){preferences=body;data=preferences}
    else if(path==='/api/claim-plan'){
      const context={week:league.currentWeek,liveWeek:league.liveWeek,market:planSettings().market,roster:mine,freeAgents:agents,rosterCapacity:league.rosterSize,protectedIds:preferences.protectedIds,verifiedAt:new Date().toISOString()};
      const review=validateClaimPlan(req.method==='PUT'?body:claimPlan,context);
      if(req.method==='PUT'){
        if(!review.valid||body.version!==claimPlan.version){res.writeHead(409,{'Content-Type':'application/json'});res.end(JSON.stringify({error:{message:review.errors.join(' ')||'Fixture version conflict'}}));return;}
        claimPlan={version:claimPlan.version+1,week:review.week,claims:review.claims,spendingLimit:review.spendingLimit,updatedAt:new Date().toISOString()};
        claimHistory.unshift({plan:structuredClone(claimPlan),receipt:null});
      }
      data={plan:claimPlan,review,processedReport:claimHistory.find(h=>h.plan.version===claimPlan.version)?.receipt||null,context:{...context,coverage:'Synthetic player pool'},espnUrl:'https://fantasy.espn.com/',submissionEnabled:false};
    }
    else if(path==='/api/claim-outcomes'){
      const current=claimHistory.find(h=>h.plan.version===claimPlan.version);
      if(req.method==='POST'){
        try{if(body.version!==claimPlan.version)throw new Error('Fixture plan version changed');const preview=reviewOfferReport(body,claimPlan,teams[0].name);if(body.save){if(current.receipt&&JSON.stringify(current.receipt)!==JSON.stringify(preview))throw new Error('Fixture report conflict');current.receipt=preview;}data={preview,saved:body.save===true};}
        catch(error){res.writeHead(400,{'Content-Type':'application/json'});res.end(JSON.stringify({error:{message:error.message}}));return;}
      }else data={plan:claimPlan,review:reconcileClaimPlan(claimPlan,mine,{updatedAt:new Date().toISOString()},current?.receipt),history:claimHistory,remainingFaab:250,espnUrl:'https://fantasy.espn.com/',explanation:'Synthetic roster observations and user-supplied reports, not independently verified receipts.'};
    }
    else if(path==='/api/trade-review')data=evaluateTradePlan(mine,theirs,body.giveIds,body.receiveIds,{...planSettings(),...preferences,...body,freeAgents:agents});
    else if(path==='/api/settings'){if(req.method==='PUT')Object.assign(settings,body);data=settings}
    else if(path==='/api/data-health')data={espn:{status:'synthetic'},playerFeatures:{count:0},schedule:{count:0}};
    else if(path==='/api/devices')data={devices:[]};
    else if(path==='/api/history')data={entries:[]};
    else if(path==='/api/decision-history'){
      const week=Number(url.searchParams.get('snapshotWeek')||4),offset=Number(url.searchParams.get('cursor')||0);
      const entries=Array.from({length:9},(_,i)=>({id:`fixture-${week}-${i}`,week,mode:'week',createdAt:`2026-09-30T10:${String(i).padStart(2,'0')}:00Z`,feedback:null}));
      data={weeks:[{week:5,count:9},{week:4,count:9},{week:3,count:9}],selectedWeek:week,entries:entries.slice(offset,offset+6),nextCursor:offset===0?'6':null,explanation:'Synthetic archive. Repeated snapshots are not independent trials.'};
    }
    else if(path==='/api/decision-replay'){
      const week=Number(url.searchParams.get('id').split('-')[1]);
      data={snapshot:{method:WAIVER_METHOD,inputs:{roster:mine,freeAgents:agents,settings:{...planSettings(),mode:'week',currentWeek:week},waiversReady:true}},actuals:Object.fromEntries([...mine,...agents].map(p=>[p.playerId,Number(p.projectedPoints)||0])),completedWeek:week<5,explanation:'Synthetic replay only.'};
    }
    else if(path==='/api/decision-feedback')data={saved:true};
    else if(path==='/api/recommendation-history')data={recorded:false};
    else if(path==='/api/injury-evidence')data={saved:true};
    else if(path==='/api/leagues/default')data={updated:true};
    else if(path.startsWith('/api/')){res.writeHead(404,{'Content-Type':'application/json'});res.end(JSON.stringify({error:{code:'FIXTURE_ONLY',message:'Not implemented in fixture'}}));return}
    if(data){res.writeHead(200,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(data));return}
    const filename=resolve(root,`.${path==='/'?'/index.html':path}`);if(!filename.startsWith(root+'/'))throw new Error('Invalid path');
    const file=await readFile(filename),mime={'.js':'text/javascript','.css':'text/css','.html':'text/html','.json':'application/json','.svg':'image/svg+xml'}[extname(filename)]||'application/octet-stream';
    const response=withSecurityHeaders(new Response(file,{headers:{'Content-Type':mime,'Cache-Control':'no-store'}}));res.writeHead(200,Object.fromEntries(response.headers));res.end(file);
  }catch{res.writeHead(500);res.end('Fixture request failed')}
});
server.listen(4180,'127.0.0.1',()=>console.log('Synthetic UI fixture: http://127.0.0.1:4180'));
process.on('SIGTERM',()=>server.close());
