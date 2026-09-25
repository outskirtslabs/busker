import { z } from "npm:zod@4.4.3";

const referenceSchema = z.strictObject({
  name: z.string().min(1),
  version: z.number().int().positive(),
});
const evidence = z.array(referenceSchema).min(1);
const text = z.string().min(1).max(12000);

export const inferenceSchemas = {
  analyze: z.discriminatedUnion("action", [
    z.strictObject({
      action: z.literal("change"),
      hypothesis: text,
      plan: z.array(text).min(1).max(12),
      evidence,
    }),
    z.strictObject({ action: z.literal("research"), question: text, evidence }),
    z.strictObject({
      action: z.literal("profile"),
      event: z.enum(["ctimer", "wall", "alloc", "jfr"]),
      question: text,
      evidence,
    }),
    z.strictObject({ action: z.literal("stop"), reason: text, evidence }),
  ]),
  research: z.strictObject({
    findings: z.array(z.strictObject({ claim: text, source: text })).max(20),
    recommendation: text,
    evidence,
  }),
  implement: z.strictObject({ summary: text, evidence }),
  repair: z.strictObject({ summary: text, evidence }),
  review: z.strictObject({
    verdict: z.enum(["pass", "fail"]),
    findings: z.array(z.strictObject({
      severity: z.enum(["blocking", "suggestion"]),
      file: text,
      description: text,
    })).max(30),
    evidence,
  }).refine(
    (result) =>
      result.verdict !== "pass" ||
      !result.findings.some((finding) => finding.severity === "blocking"),
    "A passing review cannot have blocking findings",
  ),
};

export type InferenceStage = keyof typeof inferenceSchemas;
export type OutputReference = z.infer<typeof referenceSchema>;

export interface ResourceStore {
  writeResource(
    spec: string,
    name: string,
    value: Record<string, unknown>,
  ): Promise<OutputReference>;
  readResource(
    name: string,
    version?: number,
  ): Promise<Record<string, unknown> | null>;
}

export const inferenceRecordSchema = z.strictObject({
  runId: z.string().min(1),
  step: z.number().int().positive(),
  stage: z.enum(["analyze", "research", "implement", "repair", "review"]),
  revision: z.string().regex(/^[0-9a-f]{40}$/),
  snapshotSha256: z.string().regex(/^[0-9a-f]{64}$/),
  inputs: evidence,
  raw: referenceSchema,
  status: z.enum(["valid", "invalid"]),
  error: z.string().nullable(),
  result: z.record(z.string(), z.unknown()).nullable(),
});

export type InferenceContext = Pick<
  z.infer<typeof inferenceRecordSchema>,
  "runId" | "step" | "stage" | "revision" | "snapshotSha256" | "inputs"
>;

export async function saveInference(
  store: ResourceStore,
  context: InferenceContext,
  rawOutput: string,
  exitCode: number | null,
  validationFailure?: string,
) {
  const rawHandle = await store.writeResource(
    "inferenceRaw",
    `${context.runId}-pi-${context.step}-raw`,
    { ...context, text: rawOutput, exitCode },
  );
  const raw = { name: rawHandle.name, version: rawHandle.version };
  let result: Record<string, unknown> | null = null;
  let error: string | null = null;
  try {
    if (validationFailure !== undefined) throw new Error(validationFailure);
    if (exitCode !== 0) throw new Error(`Pi exited with status ${exitCode}`);
    const parsed = inferenceSchemas[context.stage].parse(JSON.parse(rawOutput));
    const allowed = new Set(
      context.inputs.map((input) => `${input.name}@${input.version}`),
    );
    if (
      parsed.evidence.some((input) =>
        !allowed.has(`${input.name}@${input.version}`)
      )
    ) {
      throw new Error("Pi cited an output that was not in its saved inputs");
    }
    result = parsed;
  } catch (failure) {
    error = failure instanceof Error ? failure.message : String(failure);
  }
  const record = inferenceRecordSchema.parse({
    ...context,
    raw,
    status: error === null ? "valid" : "invalid",
    error,
    result,
  });
  const savedHandle = await store.writeResource(
    "inference",
    `${context.runId}-pi-${context.step}-result`,
    record,
  );
  const saved = { name: savedHandle.name, version: savedHandle.version };
  return { raw, saved, record, dataHandles: [rawHandle, savedHandle] };
}

export async function readInference(
  store: ResourceStore,
  reference: OutputReference,
  expected: InferenceContext,
) {
  const record = inferenceRecordSchema.parse(
    await store.readResource(reference.name, reference.version),
  );
  if (record.status !== "valid") {
    throw new Error("Cannot use an invalid inference output");
  }
  for (
    const field of [
      "runId",
      "step",
      "stage",
      "revision",
      "snapshotSha256",
    ] as const
  ) {
    if (record[field] !== expected[field]) {
      throw new Error(
        `Saved inference ${field} does not match`,
      );
    }
  }
  if (
    record.inputs.length !== expected.inputs.length ||
    record.inputs.some((input, index) =>
      input.name !== expected.inputs[index].name ||
      input.version !== expected.inputs[index].version
    )
  ) throw new Error("Saved inference inputs do not match");
  return inferenceSchemas[record.stage].parse(record.result);
}
