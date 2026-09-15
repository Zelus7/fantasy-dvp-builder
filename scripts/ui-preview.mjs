// Synthetic UI fixture only. Never deployed and never accesses ESPN credentials.
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {resolve,extname} from 'node:path';
import {analyzeRoster,optimizeLineup,bestAndToughestMatchups} from '../src/analysis.js';
import {recommendWaiverMoves,discoverTradeTargets,evaluateBilateralTrade} from '../src/decisions.js';
import {withSecurityHeaders} from '../src/http.js';
const settings={riskWeights:{floor:.2,median:.6,ceiling:.2},adaptiveRisk:false,fantasyPlayoffWeeks:[15,16,17]};
let preferences={protectedIds:[],watchlistIds:[]};
const slots={2:1,4:1,20:2};
const make=(id,name,position,value,extra={})=>({playerId:String(id),name,position,projectedPoints:value,seasonProjectedPoints:value*17,actualPoints:id===1?0:id===2?-2.2:null,eligibleSlotIds:[position==='RB'?2:4,23],proTeam:'BUF',injuryStatus:'ACTIVE',isStarter:id===1||id===3,lineupSlotId:position==='RB'?2:4,...extra});
const context={riskWeights:settings.riskWeights,opponentLookup:{BUF:{opponent:'MIA',game:{kickoff:'2099-09-12T17:00:00Z',eventId:'fixture'}}},dvpLookup:{'WR:MIA':{percentile:80,rank:7,grade:'A',confidence:.4},'RB:MIA':{percentile:30,rank:22,grade:'D',confidence:.4}}};
const mine=analyzeRoster([make(1,'Fixture Lead Runner','RB',20),make(2,'Fixture Bench Runner','RB',15),make(3,'Fixture Receiver','WR',5),make(4,'Fixture Injured Receiver','WR',18,{injuryStatus:'OUT',isStarter:false,lineupSlotId:20})],context);
const theirs=analyzeRoster([make(11,'Fixture Lead Receiver','WR',20),make(12,'Fixture Bench Receiver','WR',15),make(13,'Fixture Other Runner','RB',5)],context);
const agents=analyzeRoster([make(21,'Fixture Waiver Receiver','WR',12,{status:'FREEAGENT'}),make(22,'Fixture Waiver Runner','RB',8,{status:'WAIVERS'})],context);
const teams=[{id:'9',name:'Synthetic Preview Team',roster:mine},{id:'2',name:'Synthetic Other Team',roster:theirs}];
const league={leagueId:'fixture',teamId:'9',seasonYear:2026,leagueName:'SYNTHETIC TEST DATA',currentWeek:1,liveWeek:1,lineupSlotCounts:slots,rosterSize:4,isDefault:true};
const sources=()=>({espn:{status:'fresh',updatedAt:new Date().toISOString()},statistics:{status:'stale',updatedAt:'2026-01-01T00:00:00Z',throughWeek:0,coverage:'Synthetic fixture, not your live data'},news:{status:'missing',coverage:'Fixture has no news feed'}});
const workspace=week=>({generatedAt:new Date().toISOString(),league:{...league,currentWeek:week||1},team:teams[0],opponent:teams[1],roster:mine,lineup:optimizeLineup(mine,slots,mine),matchups:bestAndToughestMatchups(mine),schedules:{},actions:[{type:'lineup',title:'Review your WR depth',detail:'Synthetic fixture action'}],contingencies:[],preferences,riskWeights:settings.riskWeights,freshness:sources(),adviceReady:true,news:[],summary:{injuryAlerts:1,dataWarnings:['SYNTHETIC LOCAL PREVIEW — not live ESPN data']},changes:['Fixture roster change'],historical:false});
const root=resolve('public');
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
      data={recommendations:recommendWaiverMoves(agents.filter(p=>!position||p.position===position),mine,{mode,slots,...preferences}),trades:discoverTradeTargets(teams,'9',{slots,...preferences}),teams,watchlist:[...mine,...agents].filter(p=>preferences.watchlistIds.includes(p.playerId)),adviceReady:true,explanation:'Synthetic calculation fixture',emptyReason:'No fixture moves pass the filters',freshness:sources()};
      if(url.searchParams.get('compute')==='browser')data.calculationInput={roster:mine,freeAgents:agents.filter(p=>!position||p.position===position),teams,yourTeamId:'9',settings:{mode,slots,...preferences},includeTrades:url.searchParams.get('trades')==='true',adviceReady:true,waiversReady:true};
    }else if(path==='/api/preferences'){preferences=body;data=preferences}
    else if(path==='/api/trade-review')data=evaluateBilateralTrade(mine,theirs,body.giveIds,body.receiveIds,{slots,...preferences});
    else if(path==='/api/settings'){if(req.method==='PUT')Object.assign(settings,body);data=settings}
    else if(path==='/api/data-health')data={espn:{status:'synthetic'},playerFeatures:{count:0},schedule:{count:0}};
    else if(path==='/api/devices')data={devices:[]};
    else if(path==='/api/history')data={entries:[]};
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
