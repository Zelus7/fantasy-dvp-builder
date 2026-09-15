// ESPN weekly totals are already calculated under the league's scoring rules.
// Never substitute another week, a season total, or our model for these fields.
export function weeklyPlayerStats(player, season, week) {
  const rows = (Array.isArray(player?.stats) ? player.stats : []).filter(row =>
    Number(row.seasonId) === Number(season) &&
    Number(row.scoringPeriodId) === Number(week) &&
    Number(row.statSplitTypeId) === 1
  );
  const select = source => {
    const matches = rows.filter(row => Number(row.statSourceId) === source);
    const totals = [...new Set(matches.map(row => row.appliedTotal)
      .filter(value => value !== null && value !== undefined && value !== '')
      .map(Number).filter(Number.isFinite))];
    // Conflicting duplicate totals are not safe to pick by response order.
    return totals.length === 1 ? totals[0] : null;
  };
  return {
    actualPoints: select(0),
    projectedPoints: select(1),
    seasonProjectedPoints: (()=>{
      const values=[...new Set((player?.stats||[]).filter(row=>Number(row.seasonId)===Number(season)&&Number(row.statSourceId)===1&&Number(row.statSplitTypeId)===0)
        .map(row=>row.appliedTotal).filter(value=>value!=null&&value!==''&&Number.isFinite(Number(value))).map(Number))];
      return values.length===1?values[0]:null;
    })(),
    scorePeriod: {season: Number(season), week: Number(week)},
    scoreSource: 'ESPN league-scored weekly totals',
  };
}
