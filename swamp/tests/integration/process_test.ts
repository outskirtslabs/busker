import { deepStrictEqual, ok } from "node:assert/strict";
import { captureProcess } from "../../extensions/models/_lib/counters.ts";

Deno.test({
  name: "reads a live process and its cgroup without launching a workload",
  ignore: Deno.build.os !== "linux",
  async fn() {
    const result = await captureProcess(Deno.pid);
    deepStrictEqual(result.process.pid, Deno.pid);
    ok(result.process.startTicks > 0);
    ok(result.threads.some((thread) => thread.pid === Deno.pid));
    ok(result.process.allowedCpus);
    ok(result.cgroupPath.startsWith("/"));
    if (result.cgroupCpu !== null) ok(result.cgroupCpu.usage_usec >= 0);
    if (result.cgroupPressure !== null) {
      ok(result.cgroupPressure.some.totalUs >= 0);
    }
  },
});
