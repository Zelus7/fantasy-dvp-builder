# Compact decision preview — October 16, 2026

Scope: revise the isolated, fictional `/design-preview/`. No live advice,
credentials, transactions, or production layout changes.

## Reference review

Reviewed the actual published Sleeper mobile matchup image linked from
https://sleeper.com/fantasy-football and the rendered match list at
https://www.fotmob.com/. Sleeper places both names and points on the same row;
FotMob groups short, scannable results beneath a small control strip. These are
public examples, not a claim to have audited every screen of either native app.
Borrow their information hierarchy, not their branding, tiny text, or ads.

## Plan and critique before implementation

Six base tokens: paper `#f3f6f8`, surface `#ffffff`, ink `#173344`, muted
`#526776`, teal `#006776`, amber `#8b4a00`. Team-color marks are secondary.
Use Avenir Next / Trebuchet for compact headings, system sans for readable body
copy, and tabular numbers. Left-align names and explanations; align gains in
consistent compact chips. Support a deliberate dark palette, not accidental
browser inversion. Keep 44px controls, keyboard navigation and visible focus.

Rejected: a smaller version of the same tall card. It still repeats paragraphs,
metrics, caveats and a button for every candidate before the next name appears.

```text
Rejected                         Selected
[large heading / explanation]    [Waivers                 W6]
[horizon / search / positions]   [This week | 4 wk | Season]
[name / paragraph / metrics]    [2 targets         Filters]
[evidence / CTA / disclaimer]   [Reed RB    +2.3 if out   v]
[next player's name...]         [Replace Cole; 0.0 normal ]
                               [Ellis WR   +1.4 normal  v]
                               [Watch role; keep Grant   ]
```

Revise the initial editorial-hero direction into a decision list: the memorable
element is a player-to-player comparison, not a decorative headline. On expand,
show both names, projected points and their difference, then the exact roster
drop, what is lost, the trigger/deadline, and what missing evidence changes the
decision. Every number remains explicitly fictional. Season modes must not
reuse weekly gains or invent future totals. Summary benefits are never treated
as an automatic bid.

Acceptance: at 390×844, both collapsed candidates and their gain/condition are
visible without scrolling. No page overflow at 320px. Expanded content remains
legible with zoom, long names, keyboard-only navigation, and dark mode. Deep
links preserve filters and expanded candidates. No network or storage side
effects from the preview.

## Verification

- Local CI: 240 Node tests passed, Worker dry-run and secret scan passed.
- Chrome: 390×844 and 320×740, no horizontal overflow; both candidates fit
  above the bottom navigation without scrolling. At 390px their summary bottoms
  were 377px and 506px with the page at scroll position zero.
- Search, position/empty state, future-horizon labels, URL-preserved expansion,
  keyboard Enter expansion, dialog Escape and focus return checked.
- Expanded comparison moved ahead of the long action explanation after visual
  review. Fictional target averages use a possible three-game count (14 / 3).
- Current browser uses dark mode; the deliberate dark palette was visually
  reviewed. Light palette and reduced-motion behavior also require regression
  checks when these components are integrated into the real app.

Web Interface Guidelines review of the preview: semantic disclosures, labeled
controls, 44px actions, visible focus, tabular numbers, native modal containment,
safe-area navigation, URL filters, and reduced-motion styles are present.
This is an isolated design evaluation, not approval to replace production UI.
