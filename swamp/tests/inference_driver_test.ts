import { deepStrictEqual, match } from "node:assert/strict";
import { model } from "../extensions/models/tempo.ts";
import {
  readInferenceEffect,
  runSavedStep,
} from "../extensions/models/_lib/driver.ts";
import {
  type ResourceStore,
  saveInference,
} from "../extensions/models/_lib/inference.ts";
import { inferenceContextSchema } from "../extensions/models/_lib/schemas.ts";
import {
  newRun,
  type Outcome,
  type RunState,
} from "../extensions/models/_lib/state.ts";

class Store implements ResourceStore {
  records = new Map<string, Record<string, unknown>[]>();
  async readResource(name: string, version?: number) {
    const versions = this.records.get(name);
    return await Promise.resolve(
      structuredClone(versions?.[(version ?? versions.length) - 1] ?? null),
    );
  }
  async writeResource(
    spec: string,
    name: string,
    value: Record<string, unknown>,
  ) {
    const parsed = model.resources[spec as keyof typeof model.resources].schema
      .parse(value);
    const versions = this.records.get(name) ?? [];
    versions.push(structuredClone({ ...parsed }));
    this.records.set(name, versions);
    return await Promise.resolve({ name, version: versions.length });
  }
}
const initial = "a".repeat(40);
const limits = {
  maxAttempts: 1,
  maxRepairsPerAttempt: 2,
  maxPiCalls: 20,
  maxDurationMs: 1000,
};

function contextFor(state: Readonly<RunState>) {
  return inferenceContextSchema.parse({
    runId: state.runId,
    step: state.sequence,
    stage: state.phase,
    revision: state.workingRevision,
    snapshotSha256: "f".repeat(64),
    inputs: [{ name: "run-state", version: 1 }],
  });
}

Deno.test("saved inference decisions drive research, profiling, test repair, and review repair before keeping code", async () => {
  const store = new Store();
  let state = newRun("run", initial);
  await store.writeResource("state", "run-state", { ...state });
  async function command(outcome: Outcome) {
    const result = await runSavedStep(
      store,
      "run",
      limits,
      0,
      () => Promise.resolve({ outcome, evidence: [] }),
    );
    state = result.state;
  }
  async function inference(
    response: Record<string, unknown>,
    editedRevision?: string,
  ) {
    const result = await runSavedStep(
      store,
      "run",
      limits,
      0,
      async (active) => {
        const context = contextFor(active);
        const saved = await saveInference(
          store,
          context,
          JSON.stringify({ ...response, evidence: context.inputs }),
          0,
        );
        saved.record.result = null;
        const effect = await readInferenceEffect(
          store,
          active,
          context,
          saved.saved,
          editedRevision,
        );
        deepStrictEqual(effect.evidence, [saved.saved]);
        return effect;
      },
    );
    state = result.state;
    deepStrictEqual(state.bestRevision, initial);
    deepStrictEqual(result.record.evidence.length, 1);
  }
  await command({ kind: "prepared" });
  await command({
    kind: "measurement",
    revision: initial,
    valid: true,
    targetMet: false,
    improved: false,
  });
  await command({ kind: "attempt-started" });
  await command({ kind: "profile" });
  await inference({ action: "research", question: "Check the existing API" });
  deepStrictEqual(state.phase, "research");
  await inference({ findings: [], recommendation: "Collect wall samples" });
  deepStrictEqual(state.phase, "analyze");
  await inference({
    action: "profile",
    event: "wall",
    question: "Where does time pass?",
  });
  deepStrictEqual([state.phase, state.requestedProfile], ["profile", "wall"]);
  await command({ kind: "profile" });
  await inference({
    action: "change",
    hypothesis: "Avoid repeated work",
    plan: ["Remove the duplicate operation"],
  });
  deepStrictEqual(state.phase, "implement");
  await inference({ summary: "Changed code" }, "b".repeat(40));
  await command({
    kind: "tests",
    revision: state.workingRevision,
    passed: false,
  });
  deepStrictEqual(state.phase, "repair");
  await inference({ summary: "Fixed the failing test" }, "c".repeat(40));
  await command({
    kind: "tests",
    revision: state.workingRevision,
    passed: true,
  });
  await command({
    kind: "measurement",
    revision: state.workingRevision,
    valid: true,
    targetMet: true,
    improved: true,
  });
  await inference({
    verdict: "fail",
    findings: [{
      severity: "blocking",
      file: "src/main/example.clj",
      description: "Handle the empty input",
    }],
  });
  deepStrictEqual(state.phase, "repair");
  await inference({ summary: "Handled empty input" }, "d".repeat(40));
  deepStrictEqual([state.phase, state.targetMet], ["test", false]);
  await command({
    kind: "tests",
    revision: state.workingRevision,
    passed: true,
  });
  await command({
    kind: "measurement",
    revision: state.workingRevision,
    valid: true,
    targetMet: true,
    improved: true,
  });
  await inference({ verdict: "pass", findings: [] });
  deepStrictEqual(state.phase, "keep");
  await command({ kind: "kept", revision: state.workingRevision });
  await command({ kind: "finished" });
  deepStrictEqual([
    state.phase,
    state.bestRevision,
    state.targetMet,
    state.repairsThisAttempt,
  ], ["done", "d".repeat(40), true, 2]);
});

Deno.test("a stale passing review stops rather than keeping code and retains its evidence", async () => {
  const store = new Store();
  await store.writeResource("state", "run-state", {
    ...newRun("run", initial),
    phase: "review",
    sequence: 10,
    targetMet: true,
  });
  const result = await runSavedStep(store, "run", limits, 0, async (active) => {
    const context = contextFor(active);
    const saved = await saveInference(
      store,
      { ...context, step: context.step - 1 },
      JSON.stringify({
        verdict: "pass",
        findings: [],
        evidence: context.inputs,
      }),
      0,
    );
    return await readInferenceEffect(store, active, context, saved.saved);
  });
  deepStrictEqual([
    result.state.phase,
    result.state.targetMet,
    result.state.bestRevision,
  ], ["finish", false, initial]);
  match(result.state.stopReason ?? "", /step does not match/);
  deepStrictEqual(result.record.evidence, [{
    name: "run-pi-10-result",
    version: 1,
  }]);
});

Deno.test("an edit response without a Swamp snapshot cannot authorize tests", async () => {
  const store = new Store();
  await store.writeResource("state", "run-state", {
    ...newRun("run", initial),
    phase: "implement",
  });
  const result = await runSavedStep(store, "run", limits, 0, async (active) => {
    const context = contextFor(active);
    const saved = await saveInference(
      store,
      context,
      JSON.stringify({ summary: "Done", evidence: context.inputs }),
      0,
    );
    return await readInferenceEffect(store, active, context, saved.saved);
  });
  deepStrictEqual([
    result.state.phase,
    result.state.workingRevision,
    result.record.outcome.kind,
  ], ["finish", initial, "error"]);
  deepStrictEqual(result.record.evidence.length, 1);
});
