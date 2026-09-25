import { deepStrictEqual } from "node:assert/strict";
import {
  assessCandidate,
  type Measurement,
} from "../extensions/models/_lib/control.ts";

function measurement(id: string, h1: number, h2: number): Measurement {
  return {
    kind: "benchmark",
    id,
    revision: "a".repeat(40),
    protocolId: "same-host-and-settings",
    scores: [
      { workload: "h1", adapter: "busker", requestsPerSecond: h1, valid: true },
      {
        workload: "tls-h2",
        adapter: "busker",
        requestsPerSecond: h2,
        valid: true,
      },
    ],
  };
}
const goal = {
  kind: "improve-baseline" as const,
  multiplier: 1.5,
  workloads: ["h1"],
};
const protection = [{ workload: "tls-h2", maxDecreaseFraction: 0.05 }];
const initial = measurement("initial", 100, 200);

Deno.test("an H1 target cannot pass when H2 falls below its allowed minimum", () => {
  const result = assessCandidate(
    goal,
    protection,
    initial,
    initial,
    measurement("current", 160, 189),
  );
  deepStrictEqual([
    result.status,
    result.targetMet,
    result.improved,
    result.protectedPerformance,
  ], ["valid", false, false, false]);
  deepStrictEqual(result.protections[0].minimum, 190);
});
Deno.test("the H2 limit stays relative to the initial run, not each successive best", () => {
  const best = measurement("best", 120, 192);
  const result = assessCandidate(
    goal,
    protection,
    initial,
    best,
    measurement("current", 130, 186),
  );
  deepStrictEqual([result.improved, result.protectedPerformance], [
    false,
    false,
  ]);
});
Deno.test("a valid partial H1 improvement can be kept without claiming the target", () => {
  const result = assessCandidate(
    goal,
    protection,
    initial,
    initial,
    measurement("current", 120, 190),
  );
  deepStrictEqual([
    result.status,
    result.targetMet,
    result.improved,
    result.protectedPerformance,
  ], ["valid", false, true, true]);
});
Deno.test("H1 improvement is relative to the best tested version, not just the initial version", () => {
  const result = assessCandidate(
    goal,
    protection,
    initial,
    measurement("best", 140, 200),
    measurement("current", 130, 200),
  );
  deepStrictEqual([result.targetMet, result.improved], [false, false]);
});
Deno.test("missing or duplicate H2 scores and incompatible settings cannot authorize acceptance", () => {
  for (
    const change of ["missing", "duplicate", "settings", "invalid"] as const
  ) {
    const current = measurement("current", 160, 200);
    if (change === "missing") current.scores.pop();
    if (change === "duplicate") current.scores.push({ ...current.scores[1] });
    if (change === "settings") current.protocolId = "another-host";
    if (change === "invalid") current.scores[1].valid = false;
    const result = assessCandidate(goal, protection, initial, initial, current);
    deepStrictEqual([result.status, result.targetMet, result.improved], [
      "invalid",
      null,
      null,
    ]);
  }
});
Deno.test("protection limits are explicit and cannot disable checks with invalid numbers", () => {
  for (
    const maxDecreaseFraction of [-1, 1, Number.NaN, Number.POSITIVE_INFINITY]
  ) {
    const result = assessCandidate(
      goal,
      [{ workload: "tls-h2", maxDecreaseFraction }],
      initial,
      initial,
      measurement("current", 160, 200),
    );
    deepStrictEqual(result.status, "invalid");
  }
  const noDecrease = assessCandidate(
    goal,
    [{ workload: "tls-h2", maxDecreaseFraction: 0 }],
    initial,
    initial,
    measurement("current", 160, 199),
  );
  deepStrictEqual(noDecrease.protectedPerformance, false);
  const duplicate = assessCandidate(
    goal,
    [...protection, ...protection],
    initial,
    initial,
    measurement("current", 160, 200),
  );
  deepStrictEqual(duplicate.status, "invalid");
});

Deno.test("meeting the target cannot authorize replacing a faster best version", () => {
  const result = assessCandidate(
    goal,
    protection,
    initial,
    measurement("best", 170, 200),
    measurement("current", 160, 200),
  );
  deepStrictEqual([result.status, result.targetMet, result.improved], [
    "valid",
    false,
    false,
  ]);
});

Deno.test("H1 needs a strict practical improvement above the best while H2 keeps its original floor", () => {
  for (
    const [h1, h2, improved, targetMet] of [
      [104.99, 190, false, false],
      [105, 190, false, false],
      [105.01, 190, true, false],
      [151, 190, true, true],
      [151, 189.99, false, false],
    ] as const
  ) {
    const result = assessCandidate(
      goal,
      protection,
      initial,
      initial,
      measurement("current", h1, h2),
      0.05,
    );
    deepStrictEqual([result.improved, result.targetMet], [improved, targetMet]);
  }
  deepStrictEqual(
    assessCandidate(
      goal,
      protection,
      initial,
      initial,
      measurement("current", 104, 200),
      0.03,
    ).improved,
    true,
  );
  deepStrictEqual(
    assessCandidate(
      goal,
      protection,
      initial,
      initial,
      measurement("current", 160, 200),
      0.01,
    ).status,
    "invalid",
  );
});
