import type { Limits } from "./control.ts";
import {
  type InferenceContext,
  type OutputReference,
  readInference,
  type ResourceStore,
} from "./inference.ts";
import {
  revisionSchema,
  runIdSchema,
  runStateSchema,
  stepResultSchema,
} from "./schemas.ts";
import { beginStep, finishStep, type Outcome, type RunState } from "./state.ts";

export interface StepEffect {
  outcome: Outcome;
  evidence: OutputReference[];
}

// editedRevision comes from a Swamp Git snapshot, never from Pi's response.
export async function readInferenceEffect(
  store: ResourceStore,
  state: Readonly<RunState>,
  context: InferenceContext,
  reference: OutputReference,
  editedRevision?: string,
): Promise<StepEffect> {
  const evidence = [reference];
  try {
    if (
      state.activeStep !== state.sequence ||
      state.activeStep !== context.step ||
      state.runId !== context.runId || state.phase !== context.stage ||
      state.workingRevision !== context.revision
    ) {
      throw new Error("Inference does not match the active step");
    }
    const result = await readInference(store, reference, context);
    let outcome: Outcome;
    if ("action" in result) {
      if (result.action === "profile") {
        outcome = { kind: "analysis", action: "profile", event: result.event };
      } else if (result.action === "stop") {
        outcome = { kind: "analysis", action: "stop", reason: result.reason };
      } else {
        outcome = { kind: "analysis", action: result.action };
      }
    } else if ("verdict" in result) {
      outcome = {
        kind: "review",
        revision: state.workingRevision,
        passed: result.verdict === "pass",
      };
    } else if ("recommendation" in result) {
      outcome = { kind: "research" };
    } else {
      outcome = {
        kind: "edited",
        revision: revisionSchema.parse(editedRevision),
      };
    }
    return { outcome, evidence };
  } catch (failure) {
    return { outcome: { kind: "error", message: String(failure) }, evidence };
  }
}

async function loadState(store: ResourceStore, runId: string) {
  runIdSchema.parse(runId);
  const state = runStateSchema.parse(
    await store.readResource(`${runId}-state`),
  );
  if (state.runId !== runId) {
    throw new Error("Saved state belongs to another run");
  }
  return state;
}

// These operations require Swamp's per-model execution lock; writes are not a transaction.
export async function runSavedStep(
  store: ResourceStore,
  runId: string,
  limits: Limits,
  elapsedMs: number,
  perform: (state: Readonly<RunState>) => Promise<StepEffect>,
) {
  let active = beginStep(await loadState(store, runId), limits, elapsedMs);
  if (active.activeStep === null) active = beginStep(active, limits, elapsedMs);
  const resultName = `${runId}-step-${active.sequence}`;
  if (await store.readResource(resultName)) {
    throw new Error("Step output already exists; inspect saved progress");
  }
  const started = await store.writeResource("state", `${runId}-state`, {
    ...active,
  });
  let effect: StepEffect | undefined;
  try {
    effect = await perform(structuredClone(active));
    const record = stepResultSchema.parse({
      runId,
      step: active.sequence,
      phase: active.phase,
      revision: active.workingRevision,
      outcome: effect.outcome,
      evidence: effect.evidence,
    });
    finishStep(active, active.sequence, record.outcome, {
      name: resultName,
      version: 1,
    });
  } catch (failure) {
    const evidence = stepResultSchema.shape.evidence.safeParse(
      effect?.evidence ?? [],
    );
    effect = {
      outcome: { kind: "error", message: String(failure) },
      evidence: evidence.success ? evidence.data : [],
    };
  }
  const record = stepResultSchema.parse({
    runId,
    step: active.sequence,
    phase: active.phase,
    revision: active.workingRevision,
    outcome: effect.outcome,
    evidence: effect.evidence,
  });
  const output = await store.writeResource("step", resultName, record);
  const state = finishStep(active, active.sequence, record.outcome, {
    name: output.name,
    version: output.version,
  });
  const completed = await store.writeResource("state", `${runId}-state`, {
    ...state,
  });
  return { state, record, dataHandles: [started, output, completed] };
}

export async function recoverSavedStep(store: ResourceStore, runId: string) {
  const active = await loadState(store, runId);
  if (active.activeStep === null) {
    throw new Error("No interrupted step to recover");
  }
  const reference = { name: `${runId}-step-${active.sequence}`, version: 1 };
  const saved = await store.readResource(reference.name, reference.version);
  if (saved === null) {
    throw new Error(
      "Interrupted step has no saved result; inspect its processes and unfinished changes before proceeding",
    );
  }
  const record = stepResultSchema.parse(saved);
  if (
    record.runId !== runId || record.step !== active.activeStep ||
    record.phase !== active.phase || record.revision !== active.workingRevision
  ) {
    throw new Error("Saved result does not match the interrupted step");
  }
  const state = finishStep(active, active.sequence, record.outcome, reference);
  const completed = await store.writeResource("state", `${runId}-state`, {
    ...state,
  });
  return { state, dataHandles: [completed] };
}
