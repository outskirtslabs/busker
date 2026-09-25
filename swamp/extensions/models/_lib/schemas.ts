import { z } from "npm:zod@4.4.3";
import type { Outcome, RunState } from "./state.ts";
import { inferenceRecordSchema } from "./inference.ts";

export const runIdSchema = z.string().regex(/^[a-zA-Z0-9_-]+$/);
export const revisionSchema = z.string().regex(/^[0-9a-f]{40}$/);
const count = z.number().int().nonnegative();
export const phaseSchema = z.enum([
  "prepare",
  "baseline",
  "start-attempt",
  "profile",
  "analyze",
  "research",
  "implement",
  "test",
  "repair",
  "measure",
  "review",
  "keep",
  "discard",
  "finish",
  "done",
]);
export const runStateSchema: z.ZodType<RunState> = z.strictObject({
  runId: runIdSchema,
  phase: phaseSchema,
  initialRevision: revisionSchema,
  bestRevision: revisionSchema,
  workingRevision: revisionSchema,
  attemptsStarted: count,
  repairsThisAttempt: count,
  piCallsStarted: count,
  elapsedMs: count,
  sequence: count,
  activeStep: z.number().int().positive().nullable(),
  targetMet: z.boolean(),
  stopReason: z.string().nullable(),
  requestedProfile: z.enum(["ctimer", "wall", "alloc", "jfr"]),
  outputs: z.partialRecord(phaseSchema, inferenceRecordSchema.shape.raw),
}).refine(
  (state) => state.activeStep === null || state.activeStep === state.sequence,
  "Active step must match the saved sequence",
);

export const inferenceContextSchema = inferenceRecordSchema.pick({
  runId: true,
  step: true,
  stage: true,
  revision: true,
  snapshotSha256: true,
  inputs: true,
}).extend({ runId: runIdSchema });
export const inferenceRawSchema = inferenceContextSchema.extend({
  text: z.string(),
  exitCode: z.number().int().nullable(),
});

export const outcomeSchema: z.ZodType<Outcome> = z.union([
  z.strictObject({ kind: z.literal("prepared") }),
  z.strictObject({
    kind: z.literal("measurement"),
    revision: revisionSchema,
    valid: z.boolean(),
    inconclusive: z.boolean().optional(),
    targetMet: z.boolean(),
    improved: z.boolean(),
  }),
  z.strictObject({ kind: z.literal("attempt-started") }),
  z.strictObject({ kind: z.literal("profile") }),
  z.strictObject({
    kind: z.literal("analysis"),
    action: z.enum(["change", "research", "stop"]),
    reason: z.string().optional(),
  }),
  z.strictObject({
    kind: z.literal("analysis"),
    action: z.literal("profile"),
    event: z.enum(["ctimer", "wall", "alloc", "jfr"]),
  }),
  z.strictObject({ kind: z.literal("research") }),
  z.strictObject({ kind: z.literal("edited"), revision: revisionSchema }),
  z.strictObject({
    kind: z.literal("tests"),
    revision: revisionSchema,
    passed: z.boolean(),
  }),
  z.strictObject({
    kind: z.literal("review"),
    revision: revisionSchema,
    passed: z.boolean(),
  }),
  z.strictObject({ kind: z.literal("kept"), revision: revisionSchema }),
  z.strictObject({ kind: z.literal("discarded"), revision: revisionSchema }),
  z.strictObject({ kind: z.literal("finished") }),
  z.strictObject({ kind: z.literal("error"), message: z.string().min(1) }),
]);

export const stepResultSchema = z.strictObject({
  runId: runIdSchema,
  step: z.number().int().positive(),
  phase: phaseSchema,
  revision: revisionSchema,
  outcome: outcomeSchema,
  evidence: z.array(inferenceRecordSchema.shape.raw),
});
