import { deepStrictEqual, throws } from "node:assert/strict";
import {
  type BenchmarkExpectation,
  defaultPrecision,
  meetsPrecision,
  precisionSchema,
  precisionSummary,
  sampleSummary,
  studentTCritical,
  validateBenchmark,
} from "../extensions/models/_lib/benchmark.ts";

const expected: BenchmarkExpectation = {
  checkout: "/checkout",
  revision: "a".repeat(40),
  nativeSha256: "b".repeat(64),
  nativeResource: "file:///checkout/shim/native.so",
  sourceResource: "file:///checkout/src/main/clojure/ol/busker.clj",
  protocol: "h1",
  purpose: "measurement",
  adapter: "busker",
  parameters: {
    warmup: 10,
    duration: 30,
    repetitions: 3,
    connections: 128,
    streams: 64,
    threads: 2,
  },
};
function fixture() {
  const environment = {
    "busker-mode": "local",
    "busker-checkout": expected.checkout,
    "busker-revision": expected.revision,
    "busker-dirty?": false,
    "native-sha256": expected.nativeSha256,
    "native-version": null,
    "native-resource": "file:/checkout/shim/native.so",
    "busker-resource": expected.sourceResource,
    "java-version": "25.0.4",
    "h2load-version": "h2load 1.69.0",
    "jvm-options": [
      "-Xms512m",
      "-Xmx2g",
      "-XX:ActiveProcessorCount=2",
      "-Dbusker.bench.local=true",
    ],
    processors: 2,
    "max-heap-bytes": 2147483648,
  };
  return {
    environment,
    parameters: { ...expected.parameters, protocol: "h1", adapter: "busker" },
    results: [{
      adapter: "busker",
      protocols: {
        h1: {
          status: "ok",
          "requests-per-second": 100,
          range: [90, 110],
          samples: [90, 100, 110].map((rate) => ({
            status: "ok",
            "requests-per-second": rate,
            requests: 1000,
            response: {
              status: 200,
              body: "Hello World",
              protocol: "HTTP_1_1",
            },
            environment: structuredClone(environment),
          })),
        },
      },
    }],
  };
}
Deno.test("Student-t precision uses a fixed sample count and rejects invalid rates", () => {
  const centered = precisionSummary(
    [100, 100, 100, 100, 100, 100],
    defaultPrecision,
  );
  deepStrictEqual(
    [
      centered.mean,
      centered.sampleSD,
      centered.ciHalfWidth,
      centered.relativeHalfWidth,
    ],
    [100, 0, 0, 0],
  );
  const dispersed = precisionSummary(
    [80, 100, 100, 100, 100, 100],
    defaultPrecision,
  );
  deepStrictEqual(
    dispersed.relativeHalfWidth > defaultPrecision.relativeHalfWidth,
    true,
  );
  deepStrictEqual(meetsPrecision(centered, defaultPrecision), true);
  deepStrictEqual(meetsPrecision(dispersed, defaultPrecision), false);
  for (
    const invalid of [undefined, {}, { relativeHalfWidth: NaN }, {
      relativeHalfWidth: Infinity,
    }]
  ) {
    throws(
      () => meetsPrecision(invalid, defaultPrecision),
      /missing or nonfinite/,
    );
  }
  deepStrictEqual(sampleSummary([80, 100, 100]).median, 100);
  const tiny = precisionSummary([
    1e-300,
    2e-300,
    2e-300,
    2e-300,
    2e-300,
    2e-300,
  ], defaultPrecision);
  deepStrictEqual(tiny.relativeHalfWidth > 0, true);
  const large = precisionSummary(
    Array<number>(6).fill(Number.MAX_VALUE),
    defaultPrecision,
  );
  deepStrictEqual(large.mean, Number.MAX_VALUE);
  deepStrictEqual(large.relativeHalfWidth, 0);
  for (
    const rates of [[], [100], [0, 100], [-1, 100], [NaN, 100], [Infinity, 100]]
  ) {
    throws(() => precisionSummary(rates, defaultPrecision));
  }
});

Deno.test("Student-t critical supports confidence and sample count without a fixed table", () => {
  for (const confidence of [0.8, 0.9, 0.95, 0.99]) {
    const cauchy = Math.tan(Math.PI * confidence / 2);
    const dfTwo = Math.SQRT2 * confidence / Math.sqrt(1 - confidence ** 2);
    deepStrictEqual(
      Math.abs(studentTCritical(2, confidence) - cauchy) < 1e-8,
      true,
    );
    deepStrictEqual(
      Math.abs(studentTCritical(3, confidence) - dfTwo) < 1e-8,
      true,
    );
  }
  deepStrictEqual(
    Math.abs(studentTCritical(6, 0.95) - 2.570581835636304) < 1e-8,
    true,
  );
  // Independent Python integration used 4096 Simpson intervals.
  for (
    const [n, confidence, reference] of [
      [6, 0.9, 2.0150483733330242],
      [6, 0.99, 4.032142983555229],
      [12, 0.99, 3.105806515539299],
    ]
  ) {
    deepStrictEqual(
      Math.abs(studentTCritical(n, confidence) - reference) < 5e-8,
      true,
    );
  }
  deepStrictEqual(studentTCritical(6, 0.9) < studentTCritical(6, 0.95), true);
  deepStrictEqual(studentTCritical(6, 0.95) < studentTCritical(6, 0.99), true);
  for (const value of [0, 1, -0.1, NaN, Infinity]) {
    throws(() => studentTCritical(6, value));
  }
  throws(() => studentTCritical(1, 0.95));
  throws(() => studentTCritical(2, Number.MIN_VALUE), /numeric precision/);
  const nearOne = studentTCritical(2, 1 - Number.EPSILON);
  const cauchyTail = 2 / (Math.PI * Number.EPSILON);
  deepStrictEqual(Math.abs(nearOne / cauchyTail - 1) < 1e-10, true);
  deepStrictEqual(
    studentTCritical(6, 1 - Number.EPSILON) > studentTCritical(6, 0.99),
    true,
  );
  deepStrictEqual(precisionSchema.parse({}), defaultPrecision);
  deepStrictEqual(
    precisionSchema.safeParse({ confidenceLevel: 1 }).success,
    false,
  );
});

Deno.test("local benchmark checks source, native bytes, sample settings, and recomputed median", () => {
  const result = validateBenchmark(fixture(), expected);
  deepStrictEqual([result.kind, result.score, result.diagnosticRate], [
    "benchmark",
    null,
    100,
  ]);
});
Deno.test("explicit Busker worker count requires three matching observations per raw sample", () => {
  const report = fixture();
  const counts = { ready: 4, "measurement-start": 4, "measurement-end": 4 };
  const workerExpectation = {
    ...expected,
    parameters: { ...expected.parameters, "busker-workers": 4 },
  };
  Object.assign(report.parameters, { "busker-workers": 4 });
  for (const sample of report.results[0].protocols.h1.samples) {
    Object.assign(sample, { "busker-workers-effective": counts });
  }
  deepStrictEqual(
    validateBenchmark(report, workerExpectation).samples.length,
    3,
  );
  const sample = report.results[0].protocols.h1.samples[1];
  for (const phase of Object.keys(counts)) {
    Object.assign(sample, {
      "busker-workers-effective": { ...counts, [phase]: 2 },
    });
    throws(
      () => validateBenchmark(report, workerExpectation),
      /Effective Busker worker count/,
    );
    const missing = { ...counts };
    Reflect.deleteProperty(missing, phase);
    Object.assign(sample, { "busker-workers-effective": missing });
    throws(() => validateBenchmark(report, workerExpectation));
  }
  Reflect.deleteProperty(sample, "busker-workers-effective");
  throws(
    () => validateBenchmark(report, workerExpectation),
    /Effective Busker worker count/,
  );
  Object.assign(sample, { "busker-workers-effective": counts });
  Object.assign(report.parameters, { "busker-workers": 2 });
  throws(
    () => validateBenchmark(report, workerExpectation),
    /Wrong benchmark parameter/,
  );
});

Deno.test("smoke output cannot supply a score, even when its rates are positive", () => {
  const report = fixture();
  report.environment["busker-dirty?"] = true;
  const result = validateBenchmark(report, { ...expected, purpose: "smoke" });
  deepStrictEqual([result.kind, result.score], ["smoke", null]);
  throws(() => validateBenchmark(report, expected), /dirty source/);
});
Deno.test("invalid provenance, missing samples, wrong protocol and invented summaries cannot pass", () => {
  const corruptions: ((report: ReturnType<typeof fixture>) => void)[] = [
    (r) => {
      r.environment["busker-mode"] = "pinned";
    },
    (r) => {
      r.environment["native-resource"] = "not a URL";
    },
    (r) => {
      r.environment["busker-revision"] = "c".repeat(40);
    },
    (r) => {
      r.results[0].protocols.h1.samples[0].environment["native-sha256"] = "d"
        .repeat(64);
    },
    (r) => {
      r.results[0].protocols.h1.samples.pop();
    },
    (r) => {
      r.results[0].protocols.h1.samples[0].status = "failed";
    },
    (r) => {
      r.results[0].protocols.h1.samples[0].response.protocol = "HTTP_2";
    },
    (r) => {
      r.results[0].protocols.h1.samples[0].environment["jvm-options"] = [];
    },
    (r) => {
      r.results[0].protocols.h1["requests-per-second"] = 999;
    },
    (r) => {
      r.results[0].protocols.h1.range = [1, 999];
    },
    (r) => {
      r.parameters.duration = 1;
    },
    (r) => {
      r.results.push(structuredClone(r.results[0]));
    },
  ];
  for (const corrupt of corruptions) {
    const report = fixture();
    corrupt(report);
    throws(() => validateBenchmark(report, expected));
  }
});
