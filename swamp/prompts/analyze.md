# Analyze

Inspect the saved benchmark comparisons, profile summary, and relevant source.
If a prepared experiment brief appears in the saved inputs, use its ordered
hypotheses and exclusions to guide analysis, but check each proposal against the
current source, profiles, and measured outcomes. Choose one hypothesis per attempt;
do not bundle independent changes or treat the brief as benchmark evidence.
Choose one next action. Prefer a concrete change supported by caller stacks and
process counters. Request research or another profile only when a specific
unanswered question prevents a useful proposal. Avoid broad investigations.

Return one of these shapes, replacing every example value:

- Change:
  `{"action":"change","hypothesis":"Why this should reduce work","plan":["One concrete edit"],"evidence":[{"name":"saved-input","version":1}]}`
- Research:
  `{"action":"research","question":"The specific question","evidence":[{"name":"saved-input","version":1}]}`
- Profile:
  `{"action":"profile","event":"wall","question":"What this recording would distinguish","evidence":[{"name":"saved-input","version":1}]}`
- Stop:
  `{"action":"stop","reason":"Why no supported next step is useful","evidence":[{"name":"saved-input","version":1}]}`

Supported profile events are `ctimer`, `wall`, `alloc`, and `jfr`. Do not
propose commands or treat parked wall-time samples as proof of contention.
