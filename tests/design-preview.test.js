import test from 'node:test';
import assert from 'node:assert/strict';
import { candidates, readState, stateHash, matchingCandidates, candidateSummary } from '../public/design-preview/preview-data.js';

test('preview URL roundtrips filters, horizon and expanded players', () => {
  const s = readState('#waivers?period=bridge&position=WR&q=Jordan&open=ellis&filters=1');
  assert.deepEqual(readState(stateHash(s)), s);
  assert.equal(matchingCandidates(s)[0].id, 'ellis');
  assert.deepEqual(readState('#waivers?open=reed,reed,unknown&position=bogus&period=all').open, ['reed']);
  assert.equal(readState('#waivers?position=bogus').position, 'All');
  assert.equal(matchingCandidates(readState('#waivers?position=QB')).length, 0);
});
test('future preview views never relabel weekly point gains as future value', () => {
  for (const player of candidates) {
    assert.equal(candidateSummary(player, 'week').gain, player.gain);
    for (const period of ['bridge', 'ros']) {
      const s = candidateSummary(player, period);
      assert.equal(s.gain, 'Not ranked');
      assert.equal(s.headline, player.future);
    }
  }
});
test('sample comparisons have named drops and arithmetically consistent gains', () => {
  for (const player of candidates) {
    assert.equal((Number(player.estimate) - Number(player.baseline)).toFixed(1), Number(player.gain).toFixed(1));
    assert.ok(player.cost.includes(player.drop));
    assert.ok(player.missing && player.change && player.deadline && player.followup);
    assert.notEqual(player.drop, player.replacement);
  }
});
