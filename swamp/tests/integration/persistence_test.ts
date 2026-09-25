import { deepStrictEqual, rejects } from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { z } from "npm:zod@4.4.3";
import {
  readInference,
  type ResourceStore,
} from "../../extensions/models/_lib/inference.ts";
import { runStateSchema } from "../../extensions/models/_lib/schemas.ts";

const cwd = fileURLToPath(new URL("../../", import.meta.url));
async function swamp(args: string[]) {
  const result = await new Deno.Command("swamp", {
    cwd,
    args: [...args, "--json"],
  }).output();
  if (!result.success) throw new Error(new TextDecoder().decode(result.stderr));
  try {
    return z.record(z.string(), z.unknown()).parse(
      JSON.parse(new TextDecoder().decode(result.stdout)),
    );
  } catch (cause) {
    throw new Error("Swamp returned invalid JSON", { cause });
  }
}
async function run(method: string, input: unknown) {
  const result = await swamp([
    "model",
    "method",
    "run",
    "tempo",
    method,
    "--input",
    JSON.stringify(input),
  ]);
  deepStrictEqual(result.status, "succeeded");
}

Deno.test("Swamp saves raw and validated responses and reads their exact versions", async () => {
  const runId = `fixture-${crypto.randomUUID()}`;
  const revision = "a".repeat(40);
  await run("initialize", { runId, revision });
  const stored = await swamp([
    "data",
    "get",
    "tempo",
    `${runId}-state`,
    "--version",
    "1",
  ]);
  deepStrictEqual(runStateSchema.parse(stored.content).phase, "prepare");
  const context = {
    runId,
    step: 1,
    stage: "analyze" as const,
    revision,
    snapshotSha256: "b".repeat(64),
    inputs: [{ name: `${runId}-state`, version: 1 }],
  };
  const result = {
    action: "stop",
    reason: "Persistence fixture only",
    evidence: context.inputs,
  };
  const text = JSON.stringify(result);
  await run("recordInference", { context, text, exitCode: 0 });
  const store: ResourceStore = {
    writeResource: () =>
      Promise.reject(new Error("This integration reader cannot write")),
    readResource: async (name, version) => {
      if (version === undefined) {
        throw new Error("An explicit version is required");
      }
      const record = await swamp([
        "data",
        "get",
        "tempo",
        name,
        "--version",
        String(version),
      ]);
      return z.record(z.string(), z.unknown()).parse(record.content);
    },
  };
  const reference = { name: `${runId}-pi-1-result`, version: 1 };
  deepStrictEqual(await readInference(store, reference, context), result);
  await run("recordInference", {
    context,
    text: "incomplete response",
    exitCode: 1,
  });
  deepStrictEqual(await readInference(store, reference, context), result);
  await rejects(
    () => readInference(store, { ...reference, version: 2 }, context),
    /invalid inference/,
  );
  const raw = await store.readResource(`${runId}-pi-1-raw`, 2);
  deepStrictEqual([raw?.text, raw?.exitCode], ["incomplete response", 1]);
});
