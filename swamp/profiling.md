# Why Tempo needs more than a CPU flame graph

The Swamp collector supports thread CPU timer, wall-clock, allocation, and JFR
recordings. Short H1 and TLS H2 HTTP checks have exercised these paths. Those
checks establish functional collection, not performance or attribution quality.

## Lessons from the earlier runs

A CPU profile shows sampled execution, not all reasons a request takes time.
Earlier Busker work found several concrete problems:

- A five-second warm-up, and even the first minute after a 60-second warm-up,
  still included startup effects. Longer warm-up changed the apparent result.
  Tempo records and follows a fixed warm-up duration; it never extends a run
  until the score improves. Its current checks do not establish warm-up stability.
- The restricted CPU event missed substantial system CPU. Process counters
  assigned about 32% of JVM CPU to system time. A thread CPU timer (`ctimer`)
  exposed the response send and syscall path that the CPU graph understated.
- A wall-clock profile showed mostly expected parking in virtual-thread carriers
  and support threads. Parking percentages alone did not prove a bottleneck.
- Allocation stacks located allocation sites. JFR counter deltas supplied total
  bytes per request. Weighted allocation samples were not exact byte counts.
- Java aliases selected different collectors. Requested JVM flags were not
  enough: the runner had to capture and check effective heap and collector flags.
- A successful profiler start could still produce zero samples. This occurred
  with context-switch events under the available permissions.
- `AllowedCPUs` alone did not prove affinity. The successful replacement used
  `taskset` and checked the running JVM's affinity.
- Deleted temporary native libraries obscured names. Loading an explicit,
  checksummed library with debug symbols and checking its live mapping improved
  attribution.
- A shared server/client CPU allowance distorted cost accounting. Separate CPU
  sets and process scopes gave direct server CPU per request.

These findings came from the earlier CPU reprofile, the later profiling
calibration, and its split-client replacement. Detailed source links are in the
local work item's research note. Tempo does not depend on those local files.

## A fixed collection menu

Swamp supplies a small set of supported recordings, not arbitrary commands
constructed by Pi:

| Recording | Question | Saved output |
| --- | --- | --- |
| Thread CPU timer | Where does CPU time go? | Raw stacks, flame graph, frame and thread summaries |
| Wall-clock | Which threads wait, and where? | Raw stacks, flame graph, waits by thread |
| Allocation | Which paths allocate? | Raw weighted stacks, flame graph, allocation-site summary |
| JFR | What do allocation, GC, contention, compilation, and pinning counters show? | Recording and selected machine-readable events |

Start with one thread CPU timer recording. Pi may request a named additional
recording in its saved analysis output. Swamp validates that request and checks
limits before running it. An unavailable event fails validation with preserved
command output, rather than becoming an empty success or an unrecorded substitution.

Every recording also collects server/client CPU deltas, per-thread CPU,
context-switch and scheduler counters where readable, CPU pressure, throttling,
and exact-response checks. Missing counters are listed, not replaced with zero.
Recordings run separately so their costs do not accumulate in one sample.

## Reproducible inputs and honest limits

The collector uses an exact source revision, native-library hash, JVM and
profiler versions, workload, resource controls, and recording settings. These
facts identify a reusable profile. Matching a commit alone is insufficient.
Sampling itself is not deterministic. The commands, parsing, validity checks,
and output format can be repeatable.

An exact-response check precedes warm-up. The warm-up client exits before the
recorder starts and the measured client launches. Recordings use the benchmark's
workload settings and fixed JVM heap/processor arguments. Completed request
counts normalize diagnostic costs, but profile rates never satisfy a speed target.

Current accounting limits are explicit:

- The process-counter interval includes client startup and collector work. It is
  not an exact measurement of handler CPU alone.
- `/proc/<pid>/status` and `schedstat` describe the main thread, not process-wide
  switch or scheduler totals. Per-thread counters retain their separate scope.
- A cgroup may include both client and server. Cgroup CPU is context, not server
  CPU. The runner records affinity but does not allocate disjoint CPU sets.
- Client CPU comes from GNU time, whose precision limits very short recordings.
- Allocated bytes come from JVM total-allocation counter deltas. They include
  collector work, exclude native allocations, and are not allocation samples.
- JFR uses its `profile` configuration plus thread-allocation statistics. The
  saved JSON contains selected GC, allocation, compilation, contention, and
  virtual-thread events. No observed event does not prove no pinning or contention.
- Source, native hash and live mapping, JVM arguments, and response checks are
  validated. Native symbols and complete virtual-thread stacks are not guaranteed.

Only one measurement runs at a time. Cleanup targets processes started by that
run. It must not kill an unrelated Java process or change host permissions.

## Small summaries for Pi

The summary contains:

- total weight and its unit, plus weight per million completed requests;
- sample weight with unknown frames, missing thread labels, or stack overflow;
- exclusive leaf weights and inclusive frame weights;
- weights by thread role and complete stacks with their caller context;
- whether virtual-thread stacks contain a visible native downcall;
- limitations and links to raw evidence.

Inclusive frames overlap and cannot be added. Recursive occurrences of the same
frame count once per stack. Carrier names alone do not prove application
virtual-thread execution; a matching VirtualThread frame is also required.
Zero observed native calls does not prove the absence of such calls.

The parser retains frame names rather than merging generated names that might
refer to different functions. A later comparison can add explicit normalization
without changing the raw evidence.
