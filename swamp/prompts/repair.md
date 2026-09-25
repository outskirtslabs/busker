# Repair

Read the saved test failure or review findings and the current diff. Fix the
specific problem with the smallest change. Preserve the intended optimization
only when it remains correct. Do not alter test commands, suppress failures, or
weaken assertions merely to make checks pass. For Clojure fixes, use
`tempo_repl_eval` to reload the changed namespace and rerun focused tests;
report any remaining REPL errors.

Swamp will rerun tests and measurement after your edit, including repairs made
in response to review. Do not perform those operations yourself.

Return exactly:
`{"summary":"What was repaired and what remains unresolved","evidence":[{"name":"saved-input","version":1}]}`
