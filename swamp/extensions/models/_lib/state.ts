import { checkLimits, type Limits } from "./control.ts";
import type { OutputReference } from "./inference.ts";

export type Phase =
  | "prepare"
  | "baseline"
  | "start-attempt"
  | "profile"
  | "analyze"
  | "research"
  | "implement"
  | "test"
  | "repair"
  | "measure"
  | "review"
  | "keep"
  | "discard"
  | "finish"
  | "done";
export interface RunState {
  runId: string;
  phase: Phase;
  initialRevision: string;
  bestRevision: string;
  workingRevision: string;
  attemptsStarted: number;
  repairsThisAttempt: number;
  piCallsStarted: number;
  elapsedMs: number;
  sequence: number;
  activeStep: number | null;
  targetMet: boolean;
  stopReason: string | null;
  requestedProfile: "ctimer" | "wall" | "alloc" | "jfr";
  outputs: Partial<Record<Phase, OutputReference>>;
}

export type Outcome =
  | { kind: "prepared" }
  | {
    kind: "measurement";
    revision: string;
    valid: boolean;
    inconclusive?: boolean;
    targetMet: boolean;
    improved: boolean;
  }
  | { kind: "attempt-started" }
  | { kind: "profile" }
  | {
    kind: "analysis";
    action: "change" | "research" | "stop";
    reason?: string;
  }
  | { kind: "analysis"; action: "profile"; event: RunState["requestedProfile"] }
  | { kind: "research" }
  | { kind: "edited"; revision: string }
  | { kind: "tests"; revision: string; passed: boolean }
  | { kind: "review"; revision: string; passed: boolean }
  | { kind: "kept"; revision: string }
  | { kind: "discarded"; revision: string }
  | { kind: "finished" }
  | { kind: "error"; message: string };

const inferencePhases = new Set<Phase>([
  "analyze",
  "research",
  "implement",
  "repair",
  "review",
]);

export function newRun(runId: string, revision: string): RunState {
  if (!/^[a-zA-Z0-9_-]+$/.test(runId) || !/^[0-9a-f]{40}$/.test(revision)) {
    throw new Error("Invalid run identity");
  }
  return {
    runId,
    phase: "prepare",
    initialRevision: revision,
    bestRevision: revision,
    workingRevision: revision,
    attemptsStarted: 0,
    repairsThisAttempt: 0,
    piCallsStarted: 0,
    elapsedMs: 0,
    sequence: 0,
    activeStep: null,
    targetMet: false,
    stopReason: null,
    requestedProfile: "ctimer",
    outputs: {},
  };
}

export function beginStep(
  state: RunState,
  limits: Limits,
  elapsedMs: number,
): RunState {
  if (state.phase === "done") throw new Error("Run is already finished");
  if (state.activeStep !== null) {
    throw new Error(
      "A step is active; wait for it or inspect an interrupted run",
    );
  }
  if (elapsedMs < state.elapsedMs) {
    throw new Error("Elapsed time moved backwards");
  }
  const next = state.phase === "start-attempt"
    ? "attempt"
    : state.phase === "repair"
    ? "repair"
    : state.phase === "finish" || state.phase === "discard" ||
        state.phase === "keep"
    ? "finish"
    : inferencePhases.has(state.phase)
    ? "pi"
    : "command";
  const limit = checkLimits(
    limits,
    { ...state, elapsedMs, activeStep: false },
    next,
  );
  if (limit.action === "wrap-up") {
    const repairOnly = limit.reasons.length === 1 &&
      limit.reasons[0] === "repairs";
    return {
      ...state,
      elapsedMs,
      phase: repairOnly ? "discard" : "finish",
      stopReason: repairOnly
        ? state.stopReason
        : `limit:${limit.reasons.join(",")}`,
      activeStep: null,
    };
  }
  return {
    ...state,
    elapsedMs,
    sequence: state.sequence + 1,
    activeStep: state.sequence + 1,
    attemptsStarted: state.attemptsStarted +
      (state.phase === "start-attempt" ? 1 : 0),
    repairsThisAttempt: state.phase === "start-attempt"
      ? 0
      : state.repairsThisAttempt + (state.phase === "repair" ? 1 : 0),
    piCallsStarted: state.piCallsStarted +
      (inferencePhases.has(state.phase) ? 1 : 0),
  };
}

export function finishStep(
  state: RunState,
  step: number,
  outcome: Outcome,
  output: OutputReference,
): RunState {
  if (state.activeStep !== step || step !== state.sequence) {
    throw new Error("Completion does not match active step");
  }
  if (
    !output.name || !Number.isSafeInteger(output.version) || output.version < 1
  ) throw new Error("Completion needs a saved output reference");
  const result: RunState = {
    ...state,
    activeStep: null,
    outputs: { ...state.outputs, [state.phase]: output },
  };
  if (outcome.kind === "error") {
    return {
      ...result,
      phase: "finish",
      stopReason: `error:${outcome.message}`,
      targetMet: false,
    };
  }
  function requireKind(kind: Outcome["kind"]) {
    if (outcome.kind !== kind) {
      throw new Error(`Unexpected ${outcome.kind} result for ${state.phase}`);
    }
  }
  function sameRevision(revision: string) {
    if (revision !== state.workingRevision) {
      throw new Error("Result refers to a different code revision");
    }
  }
  switch (state.phase) {
    case "prepare":
      requireKind("prepared");
      result.phase = "baseline";
      break;
    case "baseline":
    case "measure": {
      requireKind("measurement");
      if (outcome.kind !== "measurement") break;
      sameRevision(outcome.revision);
      if (!outcome.valid) {
        if (state.phase === "measure" && outcome.inconclusive) {
          return { ...result, phase: "discard", targetMet: false };
        }
        return {
          ...result,
          phase: "finish",
          stopReason: outcome.inconclusive
            ? "measurement-inconclusive"
            : "invalid-measurement",
          targetMet: false,
        };
      }
      result.targetMet = outcome.targetMet;
      if (state.phase === "baseline") {
        result.phase = outcome.targetMet ? "finish" : "start-attempt";
        result.stopReason = outcome.targetMet ? "target-met" : null;
      } else {result.phase = outcome.improved || outcome.targetMet
          ? "review"
          : "discard";}
      break;
    }
    case "start-attempt":
      requireKind("attempt-started");
      result.phase = "profile";
      result.requestedProfile = "ctimer";
      result.targetMet = false;
      break;
    case "profile":
      requireKind("profile");
      result.phase = "analyze";
      break;
    case "analyze":
      requireKind("analysis");
      if (outcome.kind !== "analysis") break;
      if (outcome.action === "stop") {
        result.phase = "finish";
        result.stopReason = `analysis:${
          outcome.reason ?? "no further proposal"
        }`;
      } else if (outcome.action === "profile") {
        result.phase = "profile";
        result.requestedProfile = outcome.event;
      } else {result.phase = outcome.action === "change"
          ? "implement"
          : "research";}
      break;
    case "research":
      requireKind("research");
      result.phase = "analyze";
      break;
    case "implement":
    case "repair":
      requireKind("edited");
      if (outcome.kind !== "edited") break;
      if (!/^[0-9a-f]{40}$/.test(outcome.revision)) {
        throw new Error("Edited code needs a saved commit");
      }
      result.workingRevision = outcome.revision;
      result.targetMet = false;
      result.phase = "test";
      break;
    case "test":
      requireKind("tests");
      if (outcome.kind !== "tests") break;
      sameRevision(outcome.revision);
      result.phase = outcome.passed ? "measure" : "repair";
      break;
    case "review":
      requireKind("review");
      if (outcome.kind !== "review") break;
      sameRevision(outcome.revision);
      result.phase = outcome.passed ? "keep" : "repair";
      break;
    case "keep":
      requireKind("kept");
      if (outcome.kind !== "kept") break;
      sameRevision(outcome.revision);
      result.bestRevision = outcome.revision;
      result.phase = state.targetMet ? "finish" : "start-attempt";
      result.stopReason = state.targetMet ? "target-met" : null;
      break;
    case "discard":
      requireKind("discarded");
      if (outcome.kind !== "discarded") break;
      if (outcome.revision !== state.bestRevision) {
        throw new Error("Discard did not restore the best tested revision");
      }
      result.workingRevision = outcome.revision;
      result.targetMet = false;
      result.phase = "start-attempt";
      break;
    case "finish":
      requireKind("finished");
      result.phase = "done";
      result.targetMet = state.stopReason === "target-met";
      break;
    default:
      throw new Error(`Cannot complete phase ${state.phase}`);
  }
  return result;
}
