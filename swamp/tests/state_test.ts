import { deepStrictEqual, throws } from "node:assert/strict";
import {
  beginStep,
  finishStep,
  newRun,
  type Outcome,
  type RunState,
} from "../extensions/models/_lib/state.ts";

const base = "a".repeat(40);
const changed = "b".repeat(40);
const repaired = "c".repeat(40);
const limits = {
  maxAttempts: 2,
  maxRepairsPerAttempt: 1,
  maxPiCalls: 10,
  maxDurationMs: 1000,
};
function advance(state: RunState, outcome: Outcome) {
  const active = beginStep(state, limits, state.elapsedMs);
  return finishStep(active, active.sequence, outcome, {
    name: `step-${active.sequence}`,
    version: 1,
  });
}
function edited() {
  let state = newRun("run", base);
  for (
    const outcome of [
      { kind: "prepared" },
      {
        kind: "measurement",
        revision: base,
        valid: true,
        targetMet: false,
        improved: false,
      },
      { kind: "attempt-started" },
      { kind: "profile" },
      { kind: "analysis", action: "change" },
      { kind: "edited", revision: changed },
    ] satisfies Outcome[]
  ) state = advance(state, outcome);
  return state;
}

Deno.test("test failure cycles through repair and tests, without advancing the best revision", () => {
  let state = edited();
  state = advance(state, { kind: "tests", revision: changed, passed: false });
  deepStrictEqual([state.phase, state.bestRevision, state.repairsThisAttempt], [
    "repair",
    base,
    0,
  ]);
  state = advance(state, { kind: "edited", revision: repaired });
  deepStrictEqual([
    state.phase,
    state.bestRevision,
    state.workingRevision,
    state.repairsThisAttempt,
  ], ["test", base, repaired, 1]);
  state = advance(state, { kind: "tests", revision: repaired, passed: true });
  deepStrictEqual(state.phase, "measure");
});

Deno.test("review fixes invalidate the prior score and require tests plus a new measurement", () => {
  let state = advance(edited(), {
    kind: "tests",
    revision: changed,
    passed: true,
  });
  state = advance(state, {
    kind: "measurement",
    revision: changed,
    valid: true,
    targetMet: true,
    improved: true,
  });
  state = advance(state, { kind: "review", revision: changed, passed: false });
  state = advance(state, { kind: "edited", revision: repaired });
  deepStrictEqual([state.phase, state.targetMet, state.bestRevision], [
    "test",
    false,
    base,
  ]);
  throws(
    () => advance(state, { kind: "kept", revision: repaired }),
    /Unexpected/,
  );
});

Deno.test("a completed review can be saved even when elapsed time has expired", () => {
  let state = advance(edited(), {
    kind: "tests",
    revision: changed,
    passed: true,
  });
  state = advance(state, {
    kind: "measurement",
    revision: changed,
    valid: true,
    targetMet: true,
    improved: true,
  });
  state = advance(state, { kind: "review", revision: changed, passed: true });
  const active = beginStep(state, limits, 1001);
  state = finishStep(active, active.sequence, {
    kind: "kept",
    revision: changed,
  }, { name: "keep", version: 1 });
  state = advance(state, { kind: "finished" });
  deepStrictEqual([
    state.phase,
    state.bestRevision,
    state.targetMet,
    state.stopReason,
  ], ["done", changed, true, "target-met"]);
});

Deno.test("time expiry after an edit preserves its revision without claiming success", () => {
  const state = beginStep(edited(), limits, 1000);
  deepStrictEqual([
    state.phase,
    state.activeStep,
    state.workingRevision,
    state.bestRevision,
    state.stopReason,
  ], ["finish", null, changed, base, "limit:duration"]);
  const done = advance(state, { kind: "finished" });
  deepStrictEqual([done.phase, done.targetMet, done.workingRevision], [
    "done",
    false,
    changed,
  ]);
});

Deno.test("an active step cannot be replaced or completed twice", () => {
  const active = beginStep(newRun("run", base), limits, 0);
  throws(() => beginStep(active, limits, 2000), /step is active/);
  throws(
    () =>
      finishStep(active, active.sequence + 1, { kind: "prepared" }, {
        name: "bad",
        version: 1,
      }),
    /does not match/,
  );
  const finished = finishStep(active, active.sequence, { kind: "prepared" }, {
    name: "prepare",
    version: 1,
  });
  throws(
    () =>
      finishStep(finished, active.sequence, { kind: "prepared" }, {
        name: "prepare",
        version: 1,
      }),
    /does not match/,
  );
});

Deno.test("repair limit rejects that attempt, then the attempt limit stops new work", () => {
  let state = advance(edited(), {
    kind: "tests",
    revision: changed,
    passed: false,
  });
  state = advance(state, { kind: "edited", revision: repaired });
  state = advance(state, { kind: "tests", revision: repaired, passed: false });
  state = beginStep(state, limits, 10);
  deepStrictEqual([state.phase, state.bestRevision], ["discard", base]);
  state = advance(state, { kind: "discarded", revision: base });
  state = beginStep(state, { ...limits, maxAttempts: 1 }, 20);
  deepStrictEqual([state.phase, state.stopReason, state.workingRevision], [
    "finish",
    "limit:attempts",
    base,
  ]);
});

Deno.test("invalid measurement stops; stale test results cannot authorize measurement", () => {
  throws(
    () => advance(edited(), { kind: "tests", revision: base, passed: true }),
    /different code revision/,
  );
  const state = advance(
    advance(edited(), { kind: "tests", revision: changed, passed: true }),
    {
      kind: "measurement",
      revision: changed,
      valid: false,
      targetMet: true,
      improved: true,
    },
  );
  deepStrictEqual([
    state.phase,
    state.targetMet,
    state.bestRevision,
    state.stopReason,
  ], ["finish", false, base, "invalid-measurement"]);
});

Deno.test("imprecise candidate is discarded while an imprecise baseline stops", () => {
  let baseline = advance(newRun("run", base), { kind: "prepared" });
  baseline = advance(baseline, {
    kind: "measurement",
    revision: base,
    valid: false,
    inconclusive: true,
    targetMet: false,
    improved: false,
  });
  deepStrictEqual(
    [baseline.phase, baseline.stopReason, baseline.attemptsStarted],
    ["finish", "measurement-inconclusive", 0],
  );

  let candidate = advance(edited(), {
    kind: "tests",
    revision: changed,
    passed: true,
  });
  candidate = advance(candidate, {
    kind: "measurement",
    revision: changed,
    valid: false,
    inconclusive: true,
    targetMet: true,
    improved: true,
  });
  deepStrictEqual(
    [
      candidate.phase,
      candidate.stopReason,
      candidate.targetMet,
      candidate.workingRevision,
      candidate.bestRevision,
      candidate.attemptsStarted,
      candidate.piCallsStarted,
      candidate.outputs.measure,
    ],
    ["discard", null, false, changed, base, 1, 2, {
      name: "step-8",
      version: 1,
    }],
  );
  candidate = advance(candidate, { kind: "discarded", revision: base });
  deepStrictEqual(
    [candidate.phase, candidate.workingRevision, candidate.bestRevision],
    ["start-attempt", base, base],
  );
  candidate = beginStep(candidate, limits, 30);
  deepStrictEqual(
    [candidate.phase, candidate.attemptsStarted, candidate.piCallsStarted],
    ["start-attempt", 2, 2],
  );
});
Deno.test("an initial win ends the run before any Pi call", () => {
  let state = advance(newRun("run", base), { kind: "prepared" });
  state = advance(state, {
    kind: "measurement",
    revision: base,
    valid: true,
    targetMet: true,
    improved: false,
  });
  state = advance(state, { kind: "finished" });
  deepStrictEqual([
    state.phase,
    state.targetMet,
    state.piCallsStarted,
    state.attemptsStarted,
  ], ["done", true, 0, 0]);
});

Deno.test("research and extra-profile requests are limited by the Pi-call budget", () => {
  let state = advance(newRun("run", base), { kind: "prepared" });
  state = advance(state, {
    kind: "measurement",
    revision: base,
    valid: true,
    targetMet: false,
    improved: false,
  });
  state = advance(state, { kind: "attempt-started" });
  state = advance(state, { kind: "profile" });
  state = advance(state, { kind: "analysis", action: "research" });
  state = advance(state, { kind: "research" });
  state = advance(state, {
    kind: "analysis",
    action: "profile",
    event: "wall",
  });
  deepStrictEqual([state.phase, state.requestedProfile, state.piCallsStarted], [
    "profile",
    "wall",
    3,
  ]);
  state = advance(state, { kind: "profile" });
  state = beginStep(state, { ...limits, maxPiCalls: 3 }, 10);
  deepStrictEqual([state.phase, state.activeStep, state.stopReason], [
    "finish",
    null,
    "limit:pi-calls",
  ]);
});

Deno.test("cleanup failure does not report completion or erase unfinished code", () => {
  let state = beginStep(edited(), limits, 1000);
  state = advance(state, { kind: "error", message: "process still running" });
  deepStrictEqual([
    state.phase,
    state.targetMet,
    state.workingRevision,
    state.bestRevision,
    state.stopReason,
  ], ["finish", false, changed, base, "error:process still running"]);
});
