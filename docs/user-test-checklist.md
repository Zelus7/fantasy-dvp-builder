# Alex's release test checklist

Open https://fantasy-command-center.acarvall87.workers.dev/ on your phone.
These checks do not require reinstalling or pairing the connector again.

## October 14 identity, contingency and trade review

- [ ] Open Dashboard or Waivers and refresh outcomes. Names should appear for
  claims created directly in ESPN, including defenses and unrostered players.
  An unresolved name is explicitly unavailable, with its ID secondary.
- [ ] Current-week results should be separate from older worksheets and reports.
  Expand the dated archive to inspect history. A stale pending feed must not
  be presented as a confirmed empty queue or a freshly submitted claim.
- [ ] On an injury-cover target, read the named trigger and benefit versus the
  backups already owned. The normal-lineup gain must remain separate. With
  adequate cover already rostered, useless zero-gain adds should disappear.
- [ ] In Trades, expand the exchange-value explanation. Check both teams'
  weekly lineups, required extra drops, positional replacement levels and
  package imbalance. These are screens, not market prices or acceptance odds.
- [ ] Open [the separate design preview](https://fantasy-command-center.acarvall87.workers.dev/design-preview/).
  Try Dashboard, Waivers, horizon/position filters, search and disclosures on
  your phone. All players and numbers are illustrative; no button sends a claim.
  Approve or revise this direction before the production visual overhaul.

The design review used Anthropic's frontend-design skill (dbd4588f9e10) and
Vercel's web-design-guidelines skill (063bee94c3f4). They guided the mobile
information hierarchy, semantic controls, focus styles, touch targets, source
disclosures and no-overflow checks. The existing production layout is retained.

## Broader regression checks

- [ ] **Login and resume:** open the app, refresh it, switch away and return.
  You should stay signed in without a login loop. If ESPN later expires, its
  reconnect message must not log you out of the command center.
- [ ] **Lineup scores:** select Week 1 and compare a few actual scores with
  ESPN, including Drake Maye (9.82) and Bo Nix (5.44). Actual points have two
  decimals; missing values and real zeroes are different. Switch to Week 2 and
  check that opponents, projections and recommended starter slots change.
- [ ] **Waivers:** switch between This Week and Rest of Season, open a target's
  explanation and inspect its named drop alternative and lineup impact. Open
  the Waiver Edge explanation. Protect a roster player and confirm that player
  is no longer proposed as a drop. Save a watch and confirm it survives reload.
- [ ] **Trades:** inspect the rationale and lineup gain for both teams. Review
  a balanced offer and a deliberately lopsided offer in the manual analyzer.
  It should not recommend sacrificing useful depth for an unjustified upgrade.
  No suitable trade is a valid result; no offer is sent to ESPN automatically.
- [ ] **Dashboard and freshness:** review the immediate actions, roster-linked
  news and individual source timestamps. Refresh should update available inputs
  without implying that unpublished stats or complete injury coverage exist.
- [ ] **Planning and history:** inspect future bye-week gaps, recommendation
  history and changes since your last visit. A saved recommendation is not an
  executed roster transaction.
- [ ] **Phone layout:** inspect player colors/logos, details, filters and manual
  trade controls. Nothing important should be clipped or require sideways page
  scrolling; a missing logo should retain readable team-color identification.

For anything unexpected, send the page name, selected week, player or offer,
expected result and a screenshot. Do not include login or pairing codes.
