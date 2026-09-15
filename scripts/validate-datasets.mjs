// Validate normalized public NFL datasets locally before a controlled D1 import.
import {readFileSync,writeFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {replaceDvpDataset,replacePlayerFeatures,replaceScheduleDataset} from '../src/db.js';
const [directory,leagueId,seasonText,output]=process.argv.slice(2),season=Number(seasonText);
if(!directory||!/^\d+$/.test(leagueId||'')||!Number.isInteger(season)||!output)throw new Error('Usage: node scripts/validate-datasets.mjs <directory> <league-id> <season> <output.sql>');
const db=new DatabaseSync(':memory:');db.exec(readFileSync('migrations/0001_initial.sql','utf8'));
const writes=[],literal=value=>value==null?'NULL':typeof value==='number'?(Number.isFinite(value)?String(value):(()=>{throw new Error('Nonfinite value')})()):`'${String(value).replaceAll("'","''")}'`;
const env={DB:{prepare(sql){const make=(args=[])=>({bind(...values){return make(values)},async first(){return db.prepare(sql).get(...args)||null},async run(){const result=db.prepare(sql).run(...args);let index=0;writes.push(sql.replaceAll('?',()=>literal(args[index++]))+';');return{meta:{changes:Number(result.changes)}}}});return make()},async batch(statements){return Promise.all(statements.map(s=>s.run()))}}};
for(const [type,file,importer] of [['schedule',`nfl-schedule-${season}.json`,replaceScheduleDataset],['dvp',`dvp-${leagueId}-${season}.json`,replaceDvpDataset],['features',`player-features-${leagueId}-${season}.json`,replacePlayerFeatures]]){
  const data=JSON.parse(readFileSync(resolve(directory,file),'utf8'));
  if(data.metadata.season!==season||type!=='schedule'&&String(data.metadata.leagueId)!==leagueId)throw new Error('Dataset scope mismatch');
  if(type==='schedule'){
    const counts=new Map(),weeks=new Set();
    for(const game of data.rows)for(const team of [game.homeTeam,game.awayTeam]){const key=`${team}:${game.week}`;if(weeks.has(key))throw new Error('Duplicate team-week in schedule');weeks.add(key);counts.set(team,(counts.get(team)||0)+1)}
    const expected=season>=2021?17:16;if(counts.size!==32||[...counts.values()].some(n=>n!==expected))throw new Error('Incomplete regular-season schedule');
  }
  if(type==='dvp'&&(data.rows.length!==384||data.rows.some(r=>!Number.isFinite(r.pointsAllowedPerGame)||r.percentile<0||r.percentile>100)))throw new Error('Invalid or incomplete DvP coverage');
  if(type==='features'&&(data.rows.length<100||data.rows.some(r=>!Number.isFinite(r.seasonPpg))))throw new Error('Invalid or unexpectedly small feature coverage');
  const result=await importer(env,data);console.log(JSON.stringify({type,rows:result.rows,throughWeek:data.metadata.throughWeek}));
}
writeFileSync(output,writes.join('\n')+'\n');db.close();console.log(`Validated import prepared: ${output}`);
