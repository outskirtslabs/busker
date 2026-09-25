import { deepStrictEqual, doesNotThrow, throws } from "node:assert/strict";
import { verifyAcceptedComparison } from "../extensions/models/_lib/campaign.ts";
import {
  defaultPrecision,
  scorePolicyVersion,
} from "../extensions/models/_lib/benchmark.ts";
import {
  assessCandidate,
  type Measurement,
} from "../extensions/models/_lib/control.ts";

const policy = {
  goal: {
    kind: "improve-baseline" as const,
    multiplier: 1.5,
    workloads: ["h1"],
  },
  protections: [{ workload: "tls-h2", maxDecreaseFraction: 0.05 }],
  minimumImprovementFraction: 0.05,
  scorePolicyVersion,
  precision: defaultPrecision,
};
function measured(id: string, h1: number, h2 = 200): Measurement {
  return {
    kind: "benchmark",
    id,
    revision: "a".repeat(40),
    protocolId: "same-settings",
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
const initial = measured("original", 100);
const best = measured("kept", 160);

Deno.test("saved false-positive comparison cannot promote an unearned best", () => {
  const candidate = measured("candidate", 165);
  const falsePositive = assessCandidate(
    policy.goal,
    policy.protections,
    initial,
    initial,
    candidate,
    policy.minimumImprovementFraction,
  );
  deepStrictEqual([falsePositive.improved, falsePositive.targetMet], [
    true,
    true,
  ]);
  throws(
    () =>
      verifyAcceptedComparison(
        policy,
        initial,
        best,
        candidate,
        falsePositive,
        { valid: true, improved: true, targetMet: true },
      ),
    /Parent accepted comparison differs from verified scores/,
  );
  const protectedFailure = measured("unprotected", 176, 189);
  const saved = assessCandidate(
    policy.goal,
    policy.protections,
    initial,
    best,
    measured("claim", 176),
    policy.minimumImprovementFraction,
  );
  throws(
    () =>
      verifyAcceptedComparison(policy, initial, best, protectedFailure, saved, {
        valid: true,
        improved: true,
        targetMet: true,
      }),
    /Parent accepted comparison differs from verified scores/,
  );
});

Deno.test("strict best-H1 floor across three generations retains the higher requalification", () => {
  const middleRequalified = measured("middle-requalified", 167);
  const firstAssessment = assessCandidate(
    policy.goal,
    policy.protections,
    initial,
    best,
    middleRequalified,
    policy.minimumImprovementFraction,
  );
  deepStrictEqual(firstAssessment.improved, false);
  const thirdCandidate = measured("third-candidate", 175);
  const falsePositive = assessCandidate(
    policy.goal,
    policy.protections,
    initial,
    best,
    thirdCandidate,
    policy.minimumImprovementFraction,
  );
  deepStrictEqual(falsePositive.improved, true);
  throws(
    () =>
      verifyAcceptedComparison(
        policy,
        initial,
        middleRequalified,
        thirdCandidate,
        falsePositive,
        { valid: true, improved: true, targetMet: true },
      ),
    /Parent accepted comparison differs from verified scores/,
  );
  const improved = measured("earned", 176);
  const valid = assessCandidate(
    policy.goal,
    policy.protections,
    initial,
    middleRequalified,
    improved,
    policy.minimumImprovementFraction,
  );
  doesNotThrow(() =>
    verifyAcceptedComparison(
      policy,
      initial,
      middleRequalified,
      improved,
      valid,
      { valid: true, improved: true, targetMet: true },
    )
  );
});
