# Implement

Implement the single saved hypothesis and plan in the experiment worktree.
Inspect the affected source first. Keep the diff small; avoid unrelated cleanup,
new abstractions, dependency changes, and speculative fallback behavior. Add or
adjust focused correctness tests when needed. For Clojure work, use the
dedicated `tempo_repl_eval` tool to reload changed namespaces and run focused
tests while editing. Capture failures and report unresolved problems. Arbitrary
REPL evaluation can affect the JVM and filesystem; it is not a sandbox.

Swamp will snapshot the edits, run the required checks, measure performance, and
request review if the result improves. An unfinished edit is not a successful
experiment.

Return exactly:
`{"summary":"What changed and any unresolved concern","evidence":[{"name":"saved-input","version":1}]}`
