import { z } from "npm:zod@4.4.3";
import { recoverSavedStep } from "../../extensions/models/_lib/driver.ts";
import { model as tempo } from "../../extensions/models/tempo.ts";
import { supportManifest } from "../../extensions/models/_lib/support.ts";
import { benchmarkMethod } from "../../extensions/models/_lib/placement.ts";
import { inspectCheckout } from "../../extensions/models/_lib/workspace.ts";
import {
  executeRun,
  performLiveStep,
  startRun,
} from "../../extensions/models/_lib/run.ts";
import type { ArtifactStore } from "../../extensions/models/_lib/artifacts.ts";
import { join } from "node:path";

const argumentsSchema = z.strictObject({
  runId: z.string(),
  caseName: z.enum([
    "repair",
    "reject",
    "malformed",
    "expiry",
    "archive",
    "cleanup",
    "competitors",
    "candidate-noise",
    "candidate-failure",
    "recovery",
    "hold",
    "noise",
    "noise-h2",
    "noise-stabilizes",
  ]),
  repository: z.string(),
  revision: z.string(),
  directory: z.string(),
  supportDirectory: z.string(),
});
export const model = {
  ...tempo,
  type: "busker/tempo-fixture",
  methods: {
    run: {
      description:
        "Exercise real orchestration with fixed local executable fixtures and no provider calls",
      arguments: argumentsSchema,
      execute: async (
        args: z.infer<typeof argumentsSchema>,
        context: ArtifactStore,
      ) => {
        const bin = Deno.env.get("TEMPO_FIXTURE_BIN");
        if (
          !bin || Deno.env.get("PATH")?.split(":")[0] !== bin ||
          Deno.env.get("TEMPO_FIXTURE_CASE") !== args.caseName
        ) throw new Error("Fixture executables were not isolated");
        for (const name of ["bb", "pi", "clojure"]) {
          const text = await Deno.readTextFile(join(bin, name));
          if (!text.includes("this program never invokes Pi or a provider")) {
            throw new Error("Not a fixed executable fixture");
          }
        }
        let interrupt = args.caseName === "recovery";
        const store: ArtifactStore = {
          readResource: (name, version) => context.readResource(name, version),
          writeResource: async (spec, name, value) => {
            if (
              interrupt && spec === "state" && value.phase === "test" &&
              value.activeStep === null
            ) {
              interrupt = false;
              throw new Error(
                "Fixture interrupted state write after saved edit result",
              );
            }
            return await context.writeResource(
              spec,
              name,
              spec === "runReport"
                ? {
                  ...value,
                  diagnosticOnly: true,
                  fixture: true,
                  verificationTargetMet: value.targetMet,
                  targetMet: false,
                }
                : value,
            );
          },
          createFileWriter: (spec, name) => {
            const writer = context.createFileWriter(spec, name);
            if (
              args.caseName === "archive" && name.includes("-test-") &&
              !name.includes("-test-1-")
            ) {
              return {
                ...writer,
                writeStream: () =>
                  Promise.reject(new Error("Fixture archive failure")),
              };
            }
            return writer;
          },
        };
        let clock = Date.now();
        const started = await startRun(store, {
          confirmLive: true,
          runId: args.runId,
          repository: args.repository,
          revision: args.revision,
          directory: args.directory,
          supportDirectory: args.supportDirectory,
          supportSha256: (await supportManifest(args.supportDirectory)).sha256,
          supportRevision: (await inspectCheckout(args.repository)).revision,
          credentialDirectory: join(args.directory, "credentials"),
          referenceRoots: [],
          benchmarkMethod,
          limits: {
            maxAttempts: 1,
            maxRepairsPerAttempt: 2,
            maxPiCalls: 20,
            maxDurationMs: 4 * 60 * 60 * 1000,
          },
          policy: {
            goal: args.caseName === "competitors"
              ? {
                kind: "beat-all",
                competitors: {
                  h1: [
                    "aleph",
                    "capra",
                    "hirundo",
                    "http-exchange",
                    "http-kit",
                    "jetty",
                    "undertow",
                  ],
                },
              }
              : {
                kind: "improve-baseline",
                multiplier: 1.5,
                workloads: ["h1"],
              },
            protections: [{ workload: "tls-h2", maxDecreaseFraction: 0.05 }],
            minimumImprovementFraction: 0.05,
          },
          parameters: {
            warmup: 10,
            duration: 30,
            repetitions: 3,
            connections: 128,
            streams: 64,
            threads: 2,
          },
        }, clock);
        let rejectCleanup = args.caseName === "cleanup";
        const perform: Parameters<typeof executeRun>[2] = async (
          state,
          config,
        ) => {
          if (state.phase === "finish" && rejectCleanup) {
            rejectCleanup = false;
            throw new Error("Fixture cleanup failure");
          }
          const result = await performLiveStep(store, state, config);
          if (
            state.phase === "implement" &&
            ["expiry", "cleanup"].includes(args.caseName)
          ) clock += 4 * 60 * 60 * 1000 + 1;
          return result;
        };
        let result;
        try {
          result = await executeRun(store, args.runId, perform, () => clock);
        } catch (failure) {
          if (
            args.caseName === "recovery" &&
            String(failure).includes("interrupted state write")
          ) {
            await recoverSavedStep(store, args.runId);
          } else if (
            args.caseName !== "cleanup" ||
            !String(failure).includes("Cleanup failed")
          ) throw failure;
          result = await executeRun(store, args.runId, perform, () => clock);
        }
        return { dataHandles: [started.dataHandles[0], ...result.dataHandles] };
      },
    },
  },
};
