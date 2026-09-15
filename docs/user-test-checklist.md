# Alex's release test checklist

Open https://fantasy-command-center.acarvall87.workers.dev/ on your phone.
These checks do not require reinstalling or pairing the connector again.

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
