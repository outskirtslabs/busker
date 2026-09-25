import { deepStrictEqual, rejects } from "node:assert/strict";
import { join } from "node:path";
import {
  benchmarkMethod,
  placementEnvironment,
  validatePlacementEvidence,
} from "../../extensions/models/_lib/placement.ts";

Deno.test("fixed placement checks host topology and records a live pinned client process", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-affinity-" });
  try {
    const placement = await placementEnvironment(root);
    deepStrictEqual(placement.topology.map(({ cpu }) => cpu), [10, 11, 12, 13]);
    const version = await new Deno.Command("bash", {
      args: ["-c", "h2load --version"],
      env: placement.env,
    }).output();
    deepStrictEqual(version.success, true);
    const found = await new Deno.Command("python3", {
      args: ["-c", "import os; print(os.readlink('/proc/self/exe'))"],
    }).output();
    const python = new TextDecoder().decode(found.stdout).trim();
    const command = await new Deno.Command("bash", {
      args: [
        "-c",
        "h2load -c 'import sys,time; assert sys.argv[1:] == [\"a b\"]; time.sleep(0.3)' 'a b'",
      ],
      env: { ...placement.env, TEMPO_H2LOAD_REAL: python },
    }).output();
    deepStrictEqual(command.success, true);
    const [time, pid, exe, masks] =
      (await Deno.readTextFile(join(root, "client-affinity.tsv")))
        .trim().split("\t");
    deepStrictEqual(time.startsWith("20"), true);
    deepStrictEqual(exe, python);
    deepStrictEqual(masks.split(",").includes(`${pid}:12-13`), true);
    deepStrictEqual(
      masks.split(",").every((mask) => mask.endsWith(":12-13")),
      true,
    );
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});

Deno.test("every sample needs all server and client affinity snapshots", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-affinity-evidence-" });
  const sample = join(root, "sample");
  try {
    await Deno.mkdir(sample);
    const phases = ["ready", "measurement-start", "measurement-end"];
    const server = (phase: string, allowed = "10-11") => ({
      method: benchmarkMethod,
      phase,
      threads: [{ tid: 123, allowed }, { tid: 124, allowed }],
    });
    const save = (phase: string, allowed?: string) =>
      Deno.writeTextFile(
        join(sample, `server-affinity-${phase}.json`),
        JSON.stringify(server(phase, allowed)),
      );
    const realH2load = "/nix/h2load";
    const client = (mask: string) =>
      [
        `2026-09-25T00:00:01Z\t2\t${realH2load}\t2:${mask}\t-D 10 fixture`,
        `2026-09-25T00:00:02Z\t3\t${realH2load}\t3:12-13\t-D 30 fixture`,
      ].join("\n") + "\n";
    for (const phase of phases) await save(phase);
    await Deno.writeTextFile(
      join(root, "client-affinity.tsv"),
      client("12-13"),
    );
    const check = () =>
      validatePlacementEvidence(root, [sample], 10, 30, realH2load);
    await check();
    await save("measurement-start", "10-12");
    await rejects(check, /Server affinity evidence differs/);
    await save("measurement-start");
    await Deno.remove(join(sample, "server-affinity-ready.json"));
    await rejects(check, /No such file|not found/i);
    await save("ready");
    await Deno.writeTextFile(
      join(root, "client-affinity.tsv"),
      client("12-13").replace("-D 30 fixture\n", ""),
    );
    await rejects(check, /Client affinity evidence is missing/);
    await Deno.writeTextFile(
      join(root, "client-affinity.tsv"),
      client("10-11"),
    );
    await rejects(check, /Client affinity evidence is missing/);
    await Deno.writeTextFile(
      join(root, "client-affinity.tsv"),
      client("12-13").replace(realH2load, "/bin/bash"),
    );
    await rejects(check, /Client affinity evidence is missing/);
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});
