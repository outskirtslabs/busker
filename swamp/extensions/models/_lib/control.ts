export interface Limits {
  maxAttempts: number;
  maxRepairsPerAttempt: number;
  maxPiCalls: number;
  maxDurationMs: number;
}

export interface Progress {
  attemptsStarted: number;
  repairsThisAttempt: number;
  piCallsStarted: number;
  elapsedMs: number;
  activeStep: boolean;
}

export type NextStep = "attempt" | "repair" | "pi" | "command" | "finish";

function integer(value: number, minimum: number, field: string) {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new Error(`${field} must be a safe integer >= ${minimum}`);
  }
}

export function checkLimits(
  limits: Limits,
  progress: Progress,
  next: NextStep,
) {
  integer(limits.maxAttempts, 1, "maxAttempts");
  integer(limits.maxRepairsPerAttempt, 0, "maxRepairsPerAttempt");
  integer(limits.maxPiCalls, 1, "maxPiCalls");
  integer(limits.maxDurationMs, 1, "maxDurationMs");
  integer(progress.attemptsStarted, 0, "attemptsStarted");
  integer(progress.repairsThisAttempt, 0, "repairsThisAttempt");
  integer(progress.piCallsStarted, 0, "piCallsStarted");
  integer(progress.elapsedMs, 0, "elapsedMs");
  if (progress.activeStep) return { action: "wait" as const, reasons: [] };
  if (next === "finish") return { action: "continue" as const, reasons: [] };
  const reasons: string[] = [];
  if (progress.elapsedMs >= limits.maxDurationMs) reasons.push("duration");
  if (next === "attempt" && progress.attemptsStarted >= limits.maxAttempts) {
    reasons.push("attempts");
  }
  if (
    next === "repair" &&
    progress.repairsThisAttempt >= limits.maxRepairsPerAttempt
  ) reasons.push("repairs");
  if (
    ["attempt", "repair", "pi"].includes(next) &&
    progress.piCallsStarted >= limits.maxPiCalls
  ) reasons.push("pi-calls");
  return {
    action: reasons.length ? "wrap-up" as const : "continue" as const,
    reasons,
  };
}

export interface Score {
  workload: string;
  adapter: string;
  requestsPerSecond: number;
  valid: boolean;
}

export interface Measurement {
  kind: "benchmark";
  id: string;
  revision: string;
  protocolId: string;
  nativeSha256?: string;
  scores: Score[];
}

export type Goal =
  | { kind: "improve-baseline"; multiplier: number; workloads: string[] }
  | { kind: "beat-all"; competitors: Record<string, string[]> };

export function assessGoal(
  goal: Goal,
  initial: Measurement,
  current: Measurement,
) {
  const errors: string[] = [];
  if (initial.kind !== "benchmark" || current.kind !== "benchmark") {
    errors.push("Not a benchmark");
  }
  if (!initial.id || !current.id || !initial.revision || !current.revision) {
    errors.push("Missing measurement identity");
  }
  if (!initial.protocolId || initial.protocolId !== current.protocolId) {
    errors.push("Measurement protocols differ");
  }
  const workloads = goal.kind === "beat-all"
    ? Object.keys(goal.competitors)
    : goal.workloads;
  if (
    !workloads.length || new Set(workloads).size !== workloads.length ||
    workloads.some((name) => !name)
  ) {
    errors.push("Workloads must be nonempty and unique");
  }
  if (
    goal.kind === "improve-baseline" &&
    (!Number.isFinite(goal.multiplier) || goal.multiplier <= 1)
  ) {
    errors.push("Improvement multiplier must exceed one");
  }
  function score(measurement: Measurement, workload: string, adapter: string) {
    const cells = measurement.scores.filter((cell) =>
      cell.workload === workload && cell.adapter === adapter
    );
    if (
      cells.length !== 1 || !cells[0].valid ||
      !Number.isFinite(cells[0].requestsPerSecond) ||
      cells[0].requestsPerSecond <= 0
    ) {
      errors.push(
        `Missing, duplicate, or invalid score: ${measurement.id}/${workload}/${adapter}`,
      );
      return null;
    }
    return cells[0].requestsPerSecond;
  }
  const comparisons = workloads.map((workload) => {
    const baseline = score(initial, workload, "busker");
    const measured = score(current, workload, "busker");
    let target: number | null = null;
    if (goal.kind === "improve-baseline") {
      target = baseline === null ? null : baseline * goal.multiplier;
    } else {
      const adapters = goal.competitors[workload];
      if (
        !adapters.length || new Set(adapters).size !== adapters.length ||
        adapters.some((name) => !name || name === "busker")
      ) {
        errors.push(`Invalid competitor list: ${workload}`);
      } else {
        const values = adapters.map((adapter) =>
          score(initial, workload, adapter)
        );
        if (values.every((value) => value !== null)) {
          target = Math.max(...values);
        }
      }
    }
    if (target !== null && !Number.isFinite(target)) {
      errors.push(`Target exceeds numeric range: ${workload}`);
      target = null;
    }
    const met = measured !== null && target !== null &&
      (goal.kind === "beat-all" ? measured > target : measured >= target);
    return { workload, initial: baseline, measured, target, met };
  });
  return {
    kind: "goal-result" as const,
    initialMeasurement: initial.id,
    measurement: current.id,
    revision: current.revision,
    status: errors.length ? "invalid" as const : "valid" as const,
    targetMet: errors.length
      ? null
      : comparisons.every((comparison) => comparison.met),
    comparisons,
    errors,
  };
}

export interface Protection {
  workload: string;
  maxDecreaseFraction: number;
}

export function assessCandidate(
  goal: Goal,
  protections: Protection[],
  initial: Measurement,
  best: Measurement,
  current: Measurement,
  minimumImprovementFraction = 0.05,
) {
  const goalResult = assessGoal(goal, initial, current);
  const bestResult = assessGoal(goal, initial, best);
  const errors = [...goalResult.errors, ...bestResult.errors];
  if (
    !Number.isFinite(minimumImprovementFraction) ||
    minimumImprovementFraction < 0.03 || minimumImprovementFraction > 0.05
  ) errors.push("Improvement fraction must be between 3% and 5%");
  if (
    new Set(protections.map((item) => item.workload)).size !==
      protections.length
  ) {
    errors.push("Duplicate protected workloads");
  }
  const checks = protections.map(({ workload, maxDecreaseFraction }) => {
    if (
      !workload || !Number.isFinite(maxDecreaseFraction) ||
      maxDecreaseFraction < 0 || maxDecreaseFraction >= 1
    ) {
      errors.push(
        "Protected workloads require a decrease fraction from zero up to, but not including, one",
      );
    }
    const values = [initial, current].map((measurement) => {
      const matches = measurement.scores.filter((cell) =>
        cell.workload === workload && cell.adapter === "busker"
      );
      if (
        matches.length !== 1 || !matches[0].valid ||
        !Number.isFinite(matches[0].requestsPerSecond) ||
        matches[0].requestsPerSecond <= 0
      ) {
        errors.push(
          `Missing, duplicate, or invalid protected score: ${measurement.id}/${workload}`,
        );
        return null;
      }
      return matches[0].requestsPerSecond;
    });
    const [baseline, measured] = values;
    const minimum = baseline !== null && Number.isFinite(maxDecreaseFraction) &&
        maxDecreaseFraction >= 0 && maxDecreaseFraction < 1
      ? baseline * (1 - maxDecreaseFraction)
      : null;
    return {
      workload,
      initial: baseline,
      measured,
      minimum,
      passed: minimum !== null && measured !== null && measured >= minimum,
    };
  });
  const workloads = goalResult.comparisons.map((comparison) =>
    comparison.workload
  );
  const improvements = workloads.map((workload) => {
    const prior = bestResult.comparisons.find((cell) =>
      cell.workload === workload
    )?.measured ?? null;
    const measured = goalResult.comparisons.find((cell) =>
      cell.workload === workload
    )?.measured ?? null;
    return {
      workload,
      best: prior,
      measured,
      nondecreasing: prior !== null && measured !== null && measured >= prior,
      increased: prior !== null && measured !== null && measured > prior,
      meetsFloor: prior !== null && measured !== null &&
        measured > prior * (1 + minimumImprovementFraction),
    };
  });
  const protectedPerformance = checks.every((check) => check.passed);
  const meetsFloor = improvements.every((item) => item.meetsFloor);
  return {
    ...goalResult,
    status: errors.length ? "invalid" as const : "valid" as const,
    targetMet: errors.length
      ? null
      : goalResult.targetMet === true && protectedPerformance && meetsFloor,
    improved: errors.length ? null : protectedPerformance && meetsFloor,
    protectedPerformance: errors.length ? null : protectedPerformance,
    bestMeasurement: best.id,
    protections: checks,
    improvements,
    errors,
  };
}
