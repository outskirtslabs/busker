import { deepStrictEqual, rejects } from "node:assert/strict";
import {
  recoverSavedStep,
  runSavedStep,
} from "../extensions/models/_lib/driver.ts";
import type { ResourceStore } from "../extensions/models/_lib/inference.ts";
import {
  runStateSchema,
  stepResultSchema,
} from "../extensions/models/_lib/schemas.ts";
import { newRun } from "../extensions/models/_lib/state.ts";

const limits = {
  maxAttempts: 1,
  maxRepairsPerAttempt: 1,
  maxPiCalls: 3,
  maxDurationMs: 1000,
};
class Store implements ResourceStore {
  records = new Map<string, Record<string, unknown>[]>([["run-state", [{
    ...newRun("run", "a".repeat(40)),
  }]]]);
  failStateVersion = 0;
  failStep = false;
  async readResource(name: string, version?: number) {
    await Promise.resolve();
    const versions = this.records.get(name);
    return structuredClone(
      versions?.[(version ?? versions.length) - 1] ?? null,
    );
  }
  async writeResource(
    spec: string,
    name: string,
    data: Record<string, unknown>,
  ) {
    const versions = this.records.get(name) ?? [];
    if (
      (spec === "state" && versions.length + 1 === this.failStateVersion) ||
      (spec === "step" && this.failStep)
    ) throw new Error("disk full");
    const parsed = spec === "state"
      ? runStateSchema.parse(data)
      : stepResultSchema.parse(data);
    versions.push(structuredClone({ ...parsed }));
    this.records.set(name, versions);
    await Promise.resolve();
    return { name, version: versions.length };
  }
}

Deno.test("step state is saved before effects, and the saved result precedes phase advancement", async () => {
  const store = new Store();
  const result = await runSavedStep(store, "run", limits, 0, async (active) => {
    deepStrictEqual(await store.readResource("run-state"), active);
    deepStrictEqual(active.activeStep, 1);
    return { outcome: { kind: "prepared" }, evidence: [] };
  });
  deepStrictEqual(result.state.phase, "baseline");
  deepStrictEqual(result.state.outputs.prepare, {
    name: "run-step-1",
    version: 1,
  });
  deepStrictEqual((await store.readResource("run-step-1", 1))?.outcome, {
    kind: "prepared",
  });
});

Deno.test("failed reservation never starts an effect", async () => {
  const store = new Store();
  store.failStateVersion = 2;
  let calls = 0;
  await rejects(() =>
    runSavedStep(store, "run", limits, 0, () => {
      calls++;
      return Promise.resolve({ outcome: { kind: "prepared" }, evidence: [] });
    }), /disk full/);
  deepStrictEqual(calls, 0);
});

Deno.test("recovery uses a completed saved result without repeating its effect", async () => {
  const store = new Store();
  store.failStateVersion = 3;
  let calls = 0;
  await rejects(() =>
    runSavedStep(store, "run", limits, 0, () => {
      calls++;
      return Promise.resolve({ outcome: { kind: "prepared" }, evidence: [] });
    }), /disk full/);
  await rejects(() =>
    runSavedStep(store, "run", limits, 0, () => {
      calls++;
      return Promise.resolve({ outcome: { kind: "prepared" }, evidence: [] });
    }), /step is active/);
  store.failStateVersion = 0;
  const recovered = await recoverSavedStep(store, "run");
  deepStrictEqual([calls, recovered.state.phase, recovered.state.activeStep], [
    1,
    "baseline",
    null,
  ]);
  await rejects(() => recoverSavedStep(store, "run"), /No interrupted step/);
});

Deno.test("an effect without a saved result cannot be retried automatically", async () => {
  const store = new Store();
  store.failStep = true;
  await rejects(
    () =>
      runSavedStep(
        store,
        "run",
        limits,
        0,
        () => Promise.resolve({ outcome: { kind: "prepared" }, evidence: [] }),
      ),
    /disk full/,
  );
  await rejects(() => recoverSavedStep(store, "run"), /no saved result/);
  deepStrictEqual((await store.readResource("run-state"))?.activeStep, 1);
});

Deno.test("invalid step ordering is recorded as an error, not accepted as success", async () => {
  const store = new Store();
  const result = await runSavedStep(
    store,
    "run",
    limits,
    0,
    () =>
      Promise.resolve({
        outcome: { kind: "finished" },
        evidence: [{ name: "failed-action", version: 1 }],
      }),
  );
  deepStrictEqual([
    result.state.phase,
    result.record.outcome.kind,
    result.state.targetMet,
  ], ["finish", "error", false]);
  deepStrictEqual(result.record.evidence, [{
    name: "failed-action",
    version: 1,
  }]);
});

Deno.test("duration expiry selects cleanup instead of starting the next effect", async () => {
  const store = new Store();
  const result = await runSavedStep(store, "run", limits, 1000, (active) => {
    deepStrictEqual([active.phase, active.stopReason], [
      "finish",
      "limit:duration",
    ]);
    return Promise.resolve({ outcome: { kind: "finished" }, evidence: [] });
  });
  deepStrictEqual([result.state.phase, result.state.targetMet], [
    "done",
    false,
  ]);
});
