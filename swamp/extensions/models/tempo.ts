import { z } from "npm:zod@4.4.3";
import {
  inferenceRecordSchema,
  type ResourceStore,
  saveInference,
} from "./_lib/inference.ts";
import {
  inferenceContextSchema,
  inferenceRawSchema,
  revisionSchema,
  runIdSchema,
  runStateSchema,
  stepResultSchema,
} from "./_lib/schemas.ts";
import { newRun } from "./_lib/state.ts";
import { recoverSavedStep } from "./_lib/driver.ts";
import { assertNotAbandoned } from "./_lib/campaign.ts";
import {
  benchmarkRequestSchema,
  runBenchmark,
} from "./_lib/benchmark-runner.ts";
import {
  adapterSchema,
  benchmarkParametersSchema,
  precisionSchema,
  protocolSchema,
  scorePolicyVersion,
} from "./_lib/benchmark.ts";
import type { ArtifactStore } from "./_lib/artifacts.ts";
import { comparisonSchema } from "./_lib/measurement.ts";
import {
  benchmarkMethod,
  placementProvenanceSchema,
} from "./_lib/placement.ts";
import {
  profileRecordSchema,
  profileRequestSchema,
  runProfile,
} from "./_lib/profile-runner.ts";
import {
  agentsContextSchema,
  executeRun,
  experimentBriefSchema,
  performLiveStep,
  runArgumentsSchema,
  runConfigSchema,
  runDetailSchema,
  startRun,
  workspaceSchema,
} from "./_lib/run.ts";
const profileSmokeArguments = profileRequestSchema.omit({
  smoke: true,
  parameters: true,
});
const smokeArguments = benchmarkRequestSchema.pick({
  runId: true,
  repository: true,
  directory: true,
  protocol: true,
});

const initializeSchema = z.strictObject({
  runId: runIdSchema,
  revision: revisionSchema,
});
const recordSchema = z.strictObject({
  context: inferenceContextSchema,
  text: z.string(),
  exitCode: z.number().int().nullable(),
});

export const model = {
  type: "busker/tempo",
  version: "2026.09.26.1",
  globalArguments: z.strictObject({}),
  files: {
    profileFile: {
      description:
        "Raw recording, counters, collector code, and logs; excludes private keys",
      contentType: "application/octet-stream",
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    testLog: {
      description: "Complete test command output",
      contentType: "text/plain",
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    benchmarkFile: {
      description:
        "Raw benchmark results and logs; excludes generated private keys",
      contentType: "application/octet-stream",
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    piInput: {
      description: "Exact saved input values supplied to a Pi step",
      contentType: "application/json",
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    piPrompt: {
      description: "Exact Markdown instructions supplied to a Pi step",
      contentType: "text/markdown",
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    piLog: {
      description: "Complete Pi event stream or diagnostic output",
      contentType: "text/plain",
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
  },
  resources: {
    inferenceInput: {
      description:
        "Frozen untracked project AGENTS.md or optional experiment brief for inference",
      schema: z.union([agentsContextSchema, experimentBriefSchema]),
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    runConfig: {
      description: "Immutable run settings and start time",
      schema: runConfigSchema,
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    workspace: {
      description: "Isolated experiment checkout",
      schema: workspaceSchema,
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    runDetail: {
      description:
        "Versioned build, selection, cleanup, or inference handoff details",
      schema: runDetailSchema,
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    runReport: {
      description: "Final tested revision, unfinished work, and stop reason",
      schema: runDetailSchema,
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    profile: {
      description:
        "Validated diagnostic recording and counter summary, never a performance score",
      schema: profileRecordSchema,
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    comparison: {
      description:
        "Saved goal and protected-protocol comparison using exact benchmark versions",
      schema: comparisonSchema,
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    testExecution: {
      description: "Test command result, source check, and archived logs",
      schema: z.strictObject({
        runId: runIdSchema,
        step: z.number().int().positive(),
        revision: revisionSchema,
        command: z.literal("bb"),
        args: z.tuple([z.literal("qa")]),
        valid: z.boolean(),
        passed: z.boolean(),
        sourceUnchanged: z.boolean(),
        error: z.string().nullable(),
        exitCode: z.number().int().nullable(),
        signal: z.string().nullable(),
        cleanup: z.enum(["absent", "stopped", "failed"]).nullable(),
        failureOutput: z.record(
          z.enum(["stdout", "stderr"]),
          z.strictObject({
            text: z.string(),
            totalBytes: z.number().int().nonnegative(),
            truncated: z.boolean(),
          }),
        ).nullable(),
        artifacts: z.array(inferenceRecordSchema.shape.raw),
      }),
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    benchmark: {
      description:
        "Validated local benchmark or smoke check with archived evidence",
      schema: z.strictObject({
        runId: runIdSchema,
        purpose: z.enum(["smoke", "measurement"]),
        protocol: protocolSchema,
        adapter: adapterSchema,
        revision: revisionSchema,
        nativeSha256: z.string().regex(/^[a-f0-9]{64}$/),
        hostname: z.string(),
        method: z.literal(benchmarkMethod),
        scorePolicyVersion: z.literal(scorePolicyVersion),
        precision: precisionSchema,
        plannedSamples: z.union([z.literal(1), z.literal(6), z.literal(12)]),
        placement: placementProvenanceSchema,
        valid: z.boolean(),
        inconclusive: z.boolean().default(false),
        error: z.string().nullable(),
        summary: z.strictObject({
          kind: z.enum(["smoke", "benchmark"]),
          protocol: protocolSchema,
          adapter: adapterSchema,
          diagnosticRate: z.number().finite().positive(),
          score: z.number().finite().positive().nullable(),
          javaVersion: z.string(),
          h2loadVersion: z.string(),
          parameters: benchmarkParametersSchema,
          samples: z.array(z.number().finite().positive()).min(1).max(12),
          sampleSD: z.number().finite().nonnegative().optional(),
          tCritical: z.number().finite().positive().optional(),
          ciHalfWidth: z.number().finite().nonnegative().optional(),
          relativeHalfWidth: z.number().finite().nonnegative().optional(),
        }).nullable(),
        batches: z.array(z.strictObject({
          output: z.string().optional(),
          samples: z.array(z.number().finite().positive()).min(1).max(6),
        })).max(2),
        files: z.array(
          z.strictObject({
            path: z.string(),
            reference: inferenceRecordSchema.shape.raw,
          }),
        ),
        omitted: z.array(z.string()),
        logs: z.array(inferenceRecordSchema.shape.raw),
        exitCode: z.number().int().nullable(),
        cleanup: z.enum(["absent", "stopped", "failed"]).nullable(),
      }),
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    piExecution: {
      description: "Pi process result and archived evidence",
      schema: z.strictObject({
        runId: runIdSchema,
        step: z.number().int().positive(),
        revision: revisionSchema,
        success: z.boolean(),
        error: z.string().nullable(),
        exitCode: z.number().int().nullable(),
        signal: z.string().nullable(),
        cleanup: z.enum(["absent", "stopped", "failed"]).nullable(),
        modelTurns: z.number().int().nonnegative().nullable(),
        totalTokens: z.number().int().nonnegative().nullable(),
        artifacts: z.array(inferenceRecordSchema.shape.raw),
      }),
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    step: {
      description:
        "Completed step with its code revision and evidence references",
      schema: stepResultSchema,
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    state: {
      description: "Saved Tempo progress",
      schema: runStateSchema,
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    inferenceRaw: {
      description: "Unmodified Pi response and execution status",
      schema: inferenceRawSchema,
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
    inference: {
      description: "Validated Pi response or recorded validation failure",
      schema: inferenceRecordSchema,
      lifetime: "infinite",
      garbageCollection: Number.MAX_SAFE_INTEGER,
    },
  },
  methods: {
    run: {
      description:
        "Execute an explicitly authorized optimization run in an isolated worktree",
      arguments: runArgumentsSchema,
      execute: async (
        args: z.infer<typeof runArgumentsSchema>,
        context: ArtifactStore,
      ) => {
        const started = await startRun(context, args);
        const finished = await executeRun(
          context,
          args.runId,
          (state, config) => performLiveStep(context, state, config),
        );
        return {
          dataHandles: [started.dataHandles[0], ...finished.dataHandles],
        };
      },
    },
    resume: {
      description:
        "Continue a run after its interrupted step has been explicitly recovered",
      arguments: z.strictObject({
        runId: runIdSchema,
        confirmLive: z.literal(true),
      }),
      execute: async (
        args: { runId: string; confirmLive: true },
        context: ArtifactStore,
      ) => {
        const result = await executeRun(
          context,
          args.runId,
          (state, config) => performLiveStep(context, state, config),
        );
        return { dataHandles: result.dataHandles };
      },
    },
    profileSmoke: {
      description:
        "Check a one-second diagnostic recording without producing a performance score",
      arguments: profileSmokeArguments,
      execute: async (
        args: z.infer<typeof profileSmokeArguments>,
        context: ArtifactStore,
      ) => {
        const result = await runProfile(context, {
          ...args,
          smoke: true,
          parameters: {
            warmup: 1,
            duration: 1,
            connections: 128,
            streams: 64,
            threads: 2,
          },
        });
        return { dataHandles: result.dataHandles };
      },
    },
    benchmarkSmoke: {
      description:
        "Run a one-second local benchmark check; produces no performance score",
      arguments: smokeArguments,
      execute: async (
        args: z.infer<typeof smokeArguments>,
        context: ArtifactStore,
      ) => {
        const result = await runBenchmark(context, {
          ...args,
          purpose: "smoke",
          adapter: "busker",
          parameters: {
            warmup: 1,
            duration: 1,
            repetitions: 1,
            connections: 128,
            streams: 64,
            threads: 2,
          },
        });
        return { dataHandles: result.dataHandles };
      },
    },
    recover: {
      description:
        "Apply an already saved step result without repeating its effect",
      arguments: z.strictObject({ runId: runIdSchema }),
      execute: async (args: { runId: string }, context: ResourceStore) => {
        const config = runConfigSchema.parse(
          await context.readResource(`${args.runId}-config`, 1),
        );
        await assertNotAbandoned(config.arguments.directory);
        const result = await recoverSavedStep(context, args.runId);
        return { dataHandles: result.dataHandles };
      },
    },
    initialize: {
      description:
        "Save initial progress for a fresh run; does not create a worktree or execute workloads",
      arguments: initializeSchema,
      execute: async (
        args: z.infer<typeof initializeSchema>,
        context: ResourceStore,
      ) => {
        const name = `${args.runId}-state`;
        if (await context.readResource(name)) {
          throw new Error("Run already exists; use a new run ID");
        }
        const state = runStateSchema.parse(newRun(args.runId, args.revision));
        const handle = await context.writeResource("state", name, { ...state });
        return { dataHandles: [handle] };
      },
    },
    recordInference: {
      description:
        "Import a Pi response into versioned resources; does not invoke Pi",
      arguments: recordSchema,
      execute: async (
        args: z.infer<typeof recordSchema>,
        context: ResourceStore,
      ) => {
        const result = await saveInference(
          context,
          args.context,
          args.text,
          args.exitCode,
        );
        return { dataHandles: result.dataHandles };
      },
    },
  },
};
