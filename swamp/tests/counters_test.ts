import { deepStrictEqual, throws } from "node:assert/strict";
import {
  parseCpuStat,
  parsePressure,
  parseProcess,
  processCost,
} from "../extensions/models/_lib/counters.ts";

function stat(user: number, system: number, started = 500) {
  const fields = Array<string>(50).fill("0");
  fields[0] = "S";
  fields[11] = String(user);
  fields[12] = String(system);
  fields[19] = String(started);
  return `123 (worker (native)) ${fields.join(" ")}`;
}

Deno.test("process counters parse names with spaces and parentheses", () => {
  deepStrictEqual(
    parseProcess(
      stat(100, 30),
      "Cpus_allowed_list:\t4-5\nvoluntary_ctxt_switches:\t12\nnonvoluntary_ctxt_switches:\t3\n",
      "120000 4000 20",
    ),
    {
      pid: 123,
      name: "worker (native)",
      startTicks: 500,
      userTicks: 100,
      systemTicks: 30,
      voluntarySwitches: 12,
      involuntarySwitches: 3,
      schedulerRunNs: 120000,
      schedulerWaitNs: 4000,
      allowedCpus: "4-5",
    },
  );
});

Deno.test("unreadable optional counters remain null", () => {
  const before = parseProcess(stat(100, 30), null, null);
  const after = parseProcess(stat(200, 80), null, null);
  deepStrictEqual(processCost(before, after, 100, 1000), {
    pid: 123,
    startTicks: 500,
    userSeconds: 1,
    systemSeconds: 0.5,
    cpuMicrosPerRequest: 1500,
    voluntarySwitches: null,
    involuntarySwitches: null,
    schedulerRunNs: null,
    schedulerWaitNs: null,
  });
});

Deno.test("process replacement and decreasing counters cannot produce a CPU cost", () => {
  const before = parseProcess(stat(100, 30), null, null);
  throws(() =>
    processCost(before, parseProcess(stat(200, 80, 600), null, null), 100, 1000)
  );
  throws(() =>
    processCost(before, parseProcess(stat(10, 80), null, null), 100, 1000)
  );
  throws(() => processCost(before, before, 0, 1000));
  throws(() => processCost(before, before, 100, 0));
  throws(() => parseProcess("123 (java) S 1", null, null));
});

Deno.test("cgroup CPU and pressure retain their actual units", () => {
  deepStrictEqual(
    parseCpuStat(
      "usage_usec 123\nuser_usec 100\nsystem_usec 23\nnr_throttled 0\n",
    ),
    {
      usage_usec: 123,
      user_usec: 100,
      system_usec: 23,
      nr_throttled: 0,
    },
  );
  deepStrictEqual(
    parsePressure(
      "some avg10=1.50 avg60=2.00 avg300=0.50 total=450\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
    ),
    {
      some: { totalUs: 450, avg10: 1.5, avg60: 2, avg300: 0.5 },
      full: { totalUs: 0, avg10: 0, avg60: 0, avg300: 0 },
    },
  );
});

Deno.test("partial or malformed kernel records do not silently become zeros", () => {
  for (
    const input of [
      "",
      "user_usec 0",
      "usage_usec 1\nusage_usec 2",
      "usage_usec -1",
      "usage_usec NaN",
    ]
  ) throws(() => parseCpuStat(input));
  for (
    const input of [
      "",
      "some total=1",
      "some avg10=101 avg60=0 avg300=0 total=1",
      "some avg10=0 avg60=0 avg300=0 total=1 total=2",
    ]
  ) throws(() => parsePressure(input));
});
