# Review

Read the revision diff at `reviewDiff.path` in the saved manifest. Its base and
candidate commits, SHA-256, and archived artifact reference are recorded there.
Review that diff, the saved test results, benchmark comparison, and affected
source. Check correctness, lifetime and cancellation behavior, virtual-thread
use, backpressure, and whether the measured revision matches the tested code.
Check for weakened tests, changed measurement controls, and unnecessary
complexity. Do not edit files or request another reviewer.

Return exactly:
`{"verdict":"pass","findings":[],"evidence":[{"name":"saved-input","version":1}]}`

For a failure, use `"verdict":"fail"` and findings shaped as
`{"severity":"blocking","file":"path:line","description":"Concrete problem and consequence"}`.
Use `"suggestion"` only for non-blocking findings. A pass must have no blocking
findings. Keep the review focused on this change, not a general source audit.
