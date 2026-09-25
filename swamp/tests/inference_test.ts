import { deepStrictEqual, rejects } from "node:assert/strict";
import {
  type InferenceContext,
  readInference,
  type ResourceStore,
  saveInference,
} from "../extensions/models/_lib/inference.ts";

class MemoryResources implements ResourceStore {
  records = new Map<string, Record<string, unknown>[]>();
  async writeResource(
    spec: string,
    name: string,
    value: Record<string, unknown>,
  ) {
    const versions = this.records.get(name) ?? [];
    versions.push(structuredClone(value));
    this.records.set(name, versions);
    await Promise.resolve();
    return {
      name,
      version: versions.length,
      specName: spec,
      kind: "resource" as const,
    };
  }
  async readResource(name: string, version?: number) {
    await Promise.resolve();
    const versions = this.records.get(name);
    return structuredClone(
      versions?.[(version ?? versions.length) - 1] ?? null,
    );
  }
}
const context: InferenceContext = {
  runId: "run-1",
  step: 1,
  stage: "analyze",
  revision: "a".repeat(40),
  snapshotSha256: "b".repeat(64),
  inputs: [{ name: "profile", version: 1 }],
};
const result = {
  action: "change",
  hypothesis: "Reduce header allocations",
  plan: ["Remove the intermediate vector"],
  evidence: context.inputs,
};

Deno.test("next inference reads the saved version, not an in-memory result or latest version", async () => {
  const store = new MemoryResources();
  const first = await saveInference(store, context, JSON.stringify(result), 0);
  await saveInference(
    store,
    context,
    JSON.stringify({
      action: "stop",
      reason: "No further idea",
      evidence: context.inputs,
    }),
    0,
  );
  deepStrictEqual(await readInference(store, first.saved, context), result);
  deepStrictEqual(await store.readResource(first.raw.name, first.raw.version), {
    ...context,
    text: JSON.stringify(result),
    exitCode: 0,
  });
});

Deno.test("invalid and failed responses are saved but cannot feed another inference", async () => {
  for (
    const [raw, exit] of [["not JSON", 0], [JSON.stringify(result), 1], [
      JSON.stringify({
        ...result,
        evidence: [{ name: "invented", version: 1 }],
      }),
      0,
    ]] as const
  ) {
    const store = new MemoryResources();
    const saved = await saveInference(store, context, raw, exit);
    deepStrictEqual([
      saved.record.status,
      saved.record.result,
      store.records.size,
    ], ["invalid", null, 2]);
    await rejects(
      () => readInference(store, saved.saved, context),
      /invalid inference/,
    );
  }
});

Deno.test("saved output cannot be reused for another run or code snapshot", async () => {
  const store = new MemoryResources();
  const saved = await saveInference(store, context, JSON.stringify(result), 0);
  for (
    const expected of [
      { ...context, runId: "run-2" },
      {
        ...context,
        snapshotSha256: "c".repeat(64),
      },
      { ...context, revision: "d".repeat(40) },
      { ...context, step: context.step + 1 },
      { ...context, inputs: [{ name: "different", version: 1 }] },
      {
        ...context,
        inputs: context.inputs.map((input) => ({
          ...input,
          version: input.version + 1,
        })),
      },
    ]
  ) {
    await rejects(
      () => readInference(store, saved.saved, expected),
      /do(?:es)? not match/,
    );
  }
});

Deno.test("passing review with a blocking finding is invalid", async () => {
  const store = new MemoryResources();
  const saved = await saveInference(
    store,
    { ...context, stage: "review" },
    JSON.stringify({
      verdict: "pass",
      findings: [{
        severity: "blocking",
        file: "src/shim/shim.c",
        description: "Unsafe lifetime",
      }],
      evidence: context.inputs,
    }),
    0,
  );
  deepStrictEqual(saved.record.status, "invalid");
});

Deno.test("a failed save prevents a usable inference reference from being returned", async () => {
  const store = new MemoryResources();
  const failing: ResourceStore = {
    writeResource: (spec, name, value) =>
      spec === "inference"
        ? Promise.reject(new Error("disk full"))
        : store.writeResource(spec, name, value),
    readResource: (name, version) => store.readResource(name, version),
  };
  await rejects(
    () => saveInference(failing, context, JSON.stringify(result), 0),
    /disk full/,
  );
  deepStrictEqual([...store.records.keys()], ["run-1-pi-1-raw"]);
});
