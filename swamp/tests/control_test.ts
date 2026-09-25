import { deepStrictEqual, throws } from "node:assert/strict";
import {
  assessGoal,
  checkLimits,
  type Measurement,
} from "../extensions/models/_lib/control.ts";

const limits = {
  maxAttempts: 2,
  maxRepairsPerAttempt: 1,
  maxPiCalls: 5,
  maxDurationMs: 1000,
};
const progress = {
  attemptsStarted: 2,
  repairsThisAttempt: 0,
  piCallsStarted: 2,
  elapsedMs: 500,
  activeStep: false,
};

Deno.test("last allowed attempt can finish but a new one cannot start", () => {
  deepStrictEqual(checkLimits(limits, progress, "attempt"), {
    action: "wrap-up",
    reasons: ["attempts"],
  });
  for (const next of ["command", "repair", "pi", "finish"] as const) {
    deepStrictEqual(checkLimits(limits, progress, next), {
      action: "continue",
      reasons: [],
    });
  }
});

Deno.test("duration and Pi limits wait for the active step and always permit cleanup", () => {
  const expired = { ...progress, elapsedMs: 1100, piCallsStarted: 5 };
  deepStrictEqual(checkLimits(limits, { ...expired, activeStep: true }, "pi"), {
    action: "wait",
    reasons: [],
  });
  deepStrictEqual(checkLimits(limits, expired, "pi"), {
    action: "wrap-up",
    reasons: ["duration", "pi-calls"],
  });
  deepStrictEqual(checkLimits(limits, expired, "finish"), {
    action: "continue",
    reasons: [],
  });
});

Deno.test("repair count is scoped to the attempt and invalid limits are refused", () => {
  deepStrictEqual(
    checkLimits(limits, { ...progress, repairsThisAttempt: 1 }, "repair"),
    { action: "wrap-up", reasons: ["repairs"] },
  );
  throws(() => checkLimits({ ...limits, maxDurationMs: NaN }, progress, "pi"));
  throws(() => checkLimits(limits, { ...progress, elapsedMs: -1 }, "command"));
});

const initial: Measurement = {
  kind: "benchmark",
  id: "initial",
  revision: "base",
  protocolId: "fixed-protocol",
  scores: [
    { workload: "h1", adapter: "busker", requestsPerSecond: 100, valid: true },
    { workload: "h1", adapter: "jetty", requestsPerSecond: 140, valid: true },
    {
      workload: "h1",
      adapter: "http-kit",
      requestsPerSecond: 150,
      valid: true,
    },
    { workload: "h2", adapter: "busker", requestsPerSecond: 200, valid: true },
    { workload: "h2", adapter: "jetty", requestsPerSecond: 250, valid: true },
  ],
};
const current: Measurement = {
  ...initial,
  id: "measurement-1",
  revision: "changed",
  scores: [
    { workload: "h1", adapter: "busker", requestsPerSecond: 150, valid: true },
    { workload: "h2", adapter: "busker", requestsPerSecond: 300, valid: true },
  ],
};

Deno.test("multiplier goal checks every workload against unchanged initial scores", () => {
  deepStrictEqual(
    assessGoal(
      { kind: "improve-baseline", multiplier: 1.5, workloads: ["h1", "h2"] },
      initial,
      current,
    ),
    {
      kind: "goal-result",
      initialMeasurement: "initial",
      measurement: "measurement-1",
      revision: "changed",
      status: "valid",
      targetMet: true,
      comparisons: [
        { workload: "h1", initial: 100, measured: 150, target: 150, met: true },
        { workload: "h2", initial: 200, measured: 300, target: 300, met: true },
      ],
      errors: [],
    },
  );
});

Deno.test("beating all competitors requires a strict win, not a tie or an average win", () => {
  const result = assessGoal(
    {
      kind: "beat-all",
      competitors: { h1: ["jetty", "http-kit"], h2: ["jetty"] },
    },
    initial,
    current,
  );
  deepStrictEqual([
    result.status,
    result.targetMet,
    result.comparisons.map((cell) => cell.met),
  ], ["valid", false, [false, true]]);
});

Deno.test("the initial measurement can already meet the competition target", () => {
  const alreadyFast = {
    ...initial,
    scores: initial.scores.map((cell) =>
      cell.adapter === "busker" ? { ...cell, requestsPerSecond: 1000 } : cell
    ),
  };
  const result = assessGoal(
    { kind: "beat-all", competitors: { h1: ["jetty", "http-kit"] } },
    alreadyFast,
    alreadyFast,
  );
  deepStrictEqual([result.status, result.targetMet], ["valid", true]);
});

Deno.test("missing, duplicate, failed or incompatible data cannot satisfy a target", () => {
  const goal = {
    kind: "improve-baseline" as const,
    multiplier: 1.5,
    workloads: ["h1", "h2"],
  };
  for (
    const measurement of [
      { ...current, scores: [] },
      { ...current, scores: [...current.scores, current.scores[0]] },
      {
        ...current,
        scores: current.scores.map((cell) => ({ ...cell, valid: false })),
      },
      { ...current, protocolId: "changed-controls" },
      {
        ...current,
        scores: current.scores.map((cell) => ({
          ...cell,
          requestsPerSecond: Infinity,
        })),
      },
    ]
  ) {
    const result = assessGoal(goal, initial, measurement);
    deepStrictEqual(
      [result.status, result.targetMet, result.errors.length > 0],
      ["invalid", null, true],
    );
  }
  const missingCompetitor = assessGoal(
    { kind: "beat-all", competitors: { h1: ["missing"] } },
    initial,
    current,
  );
  deepStrictEqual([missingCompetitor.status, missingCompetitor.targetMet], [
    "invalid",
    null,
  ]);
});
