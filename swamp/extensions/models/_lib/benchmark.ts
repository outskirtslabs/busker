import { z } from "npm:zod@4.4.3";

export const protocolSchema = z.enum(["h1", "tls-h1", "tls-h2"]);
export const adapterSchema = z.enum([
  "busker",
  "aleph",
  "capra",
  "hirundo",
  "http-exchange",
  "http-kit",
  "jetty",
  "undertow",
]);
export const benchmarkParametersSchema = z.strictObject({
  warmup: z.number().int().positive(),
  duration: z.number().int().positive(),
  repetitions: z.number().int().positive(),
  connections: z.number().int().positive(),
  streams: z.number().int().positive(),
  threads: z.number().int().positive(),
  "busker-workers": z.number().int().positive().optional(),
});
export const scoredRepetitionsSchema = z.union([z.literal(3), z.literal(6)]);
export const scorePolicyVersion = "arithmetic-mean-student-t-v1" as const;
export const defaultPrecision = {
  relativeHalfWidth: 0.05,
  confidenceLevel: 0.95,
};
export const precisionSchema = z.strictObject({
  relativeHalfWidth: z.number().finite().gt(0).lt(1).default(0.05),
  confidenceLevel: z.number().finite().gt(0).lt(1).default(0.95),
});
const environmentSchema = z.object({
  "busker-mode": z.literal("local"),
  "busker-checkout": z.string(),
  "busker-revision": z.string(),
  "busker-dirty?": z.boolean(),
  "native-sha256": z.string().regex(/^[a-f0-9]{64}$/),
  "native-version": z.null(),
  "native-resource": z.string(),
  "busker-resource": z.string(),
  "java-version": z.string(),
  "jvm-options": z.array(z.string()),
  processors: z.number().int().positive(),
  "max-heap-bytes": z.number().positive(),
});
const sampleSchema = z.object({
  status: z.literal("ok"),
  "requests-per-second": z.number().positive(),
  requests: z.number().int().positive(),
  environment: environmentSchema,
  response: z.object({
    status: z.literal(200),
    body: z.literal("Hello World"),
    protocol: z.string(),
  }),
  "busker-workers-effective": z.strictObject({
    ready: z.number().int().positive(),
    "measurement-start": z.number().int().positive(),
    "measurement-end": z.number().int().positive(),
  }).optional(),
});
const reportSchema = z.object({
  environment: environmentSchema.extend({ "h2load-version": z.string() }),
  parameters: z.object({
    ...benchmarkParametersSchema.shape,
    protocol: protocolSchema,
    adapter: adapterSchema,
  }),
  results: z.array(
    z.object({
      adapter: z.string(),
      protocols: z.record(
        z.string(),
        z.object({
          status: z.literal("ok"),
          "requests-per-second": z.number().positive(),
          range: z.tuple([z.number().positive(), z.number().positive()]),
          samples: z.array(sampleSchema).min(1),
        }),
      ),
    }),
  ),
});
export interface BenchmarkExpectation {
  checkout: string;
  revision: string;
  nativeSha256: string;
  nativeResource: string;
  sourceResource: string;
  protocol: z.infer<typeof protocolSchema>;
  adapter: z.infer<typeof adapterSchema>;
  purpose: "smoke" | "measurement";
  parameters: z.infer<typeof benchmarkParametersSchema>;
}

export function resourceURL(value: string) {
  try {
    return new URL(value).href;
  } catch (cause) {
    throw new Error("Invalid resource URL", { cause });
  }
}

export function sampleSummary(samples: number[]) {
  if (
    !samples.length ||
    samples.some((rate) => !Number.isFinite(rate) || rate <= 0)
  ) {
    throw new Error("Benchmark needs positive finite sample rates");
  }
  const rates = [...samples].sort((a, b) => a - b);
  const midpoint = Math.floor(rates.length / 2);
  const median = rates.length % 2
    ? rates[midpoint]
    : (rates[midpoint - 1] + rates[midpoint]) / 2;
  return { rates, median };
}

function cosinePowerIntegral(angle: number, power: number) {
  let even = angle;
  let odd = Math.sin(angle);
  const sine = Math.sin(angle);
  const cosine = Math.cos(angle);
  for (let exponent = 2; exponent <= power; exponent++) {
    const previous = exponent % 2 ? odd : even;
    const current = (sine * cosine ** (exponent - 1) +
      (exponent - 1) * previous) / exponent;
    if (exponent % 2) odd = current;
    else even = current;
  }
  return power % 2 ? odd : even;
}

function sinePowerIntegral(angle: number, power: number) {
  const panels = 128;
  if (power === 0) return angle;
  if (power === 1) return 2 * Math.sin(angle / 2) ** 2;
  const step = angle / panels;
  let sum = Math.sin(0) ** power + Math.sin(angle) ** power;
  for (let index = 1; index < panels; index++) {
    sum += (index % 2 ? 4 : 2) * Math.sin(index * step) ** power;
  }
  return sum * step / 3;
}

export function studentTCritical(sampleCount: number, confidenceLevel: number) {
  if (
    !Number.isSafeInteger(sampleCount) || sampleCount < 2 ||
    !Number.isFinite(confidenceLevel) || confidenceLevel <= 0 ||
    confidenceLevel >= 1
  ) {
    throw new Error(
      "Student-t needs at least two samples and confidence in (0, 1)",
    );
  }
  const degreesOfFreedom = sampleCount - 1;
  const power = degreesOfFreedom - 1;
  // t = sqrt(df) * tan(theta) turns the Student-t density into cos(theta)^power.
  const total = cosinePowerIntegral(Math.PI / 2, power);
  const useHead = confidenceLevel < 0.5;
  const target = total * (useHead ? confidenceLevel : 1 - confidenceLevel);
  if (!(target > 0 && target < total)) {
    throw new Error("Confidence cannot be resolved at numeric precision");
  }
  let lower = 0;
  let upper = Math.PI / 2;
  // The complementary tail avoids subtracting nearly equal areas near confidence 1.
  for (let index = 0; index < 96; index++) {
    const midpoint = (lower + upper) / 2;
    const area = useHead
      ? cosinePowerIntegral(midpoint, power)
      : sinePowerIntegral(midpoint, power);
    if (area < target) lower = midpoint;
    else upper = midpoint;
  }
  const critical = useHead
    ? Math.sqrt(degreesOfFreedom) * Math.tan(upper)
    : Math.sqrt(degreesOfFreedom) / Math.tan(lower);
  const achieved = useHead
    ? cosinePowerIntegral(upper, power)
    : sinePowerIntegral(lower, power);
  if (Math.abs(achieved - target) > target * 1e-6) {
    throw new Error("Confidence cannot be resolved at numeric precision");
  }
  if (!Number.isFinite(critical) || critical <= 0) {
    throw new Error("Student-t critical value is not finite");
  }
  return critical;
}

export function precisionSummary(
  samples: number[],
  precision: z.infer<typeof precisionSchema>,
) {
  const checkedPrecision = precisionSchema.parse(precision);
  const { rates } = sampleSummary(samples);
  if (rates.length < 2) {
    throw new Error("Precision requires at least two samples");
  }
  const scale = rates[rates.length - 1];
  const normalizedRates = rates.map((rate) => rate / scale);
  const total = rates.reduce((sum, rate) => sum + rate, 0);
  const mean = Number.isFinite(total)
    ? total / rates.length
    : normalizedRates.reduce((sum, rate) => sum + rate, 0) / rates.length *
      scale;
  const normalizedMean = mean / scale;
  const normalizedSumSquares = normalizedRates.reduce(
    (sum, rate) => sum + (rate - normalizedMean) ** 2,
    0,
  );
  const normalizedSD = Math.sqrt(normalizedSumSquares / (rates.length - 1));
  const sampleSD = normalizedSD * scale;
  const tCritical = studentTCritical(
    rates.length,
    checkedPrecision.confidenceLevel,
  );
  const relativeHalfWidth = tCritical * normalizedSD /
    (Math.sqrt(rates.length) * normalizedMean);
  const ciHalfWidth = relativeHalfWidth * mean;
  if (
    !Number.isFinite(mean) || mean <= 0 || !Number.isFinite(sampleSD) ||
    !Number.isFinite(ciHalfWidth) || !Number.isFinite(relativeHalfWidth)
  ) throw new Error("Precision calculation is not finite");
  return { rates, mean, sampleSD, tCritical, ciHalfWidth, relativeHalfWidth };
}

export function meetsPrecision(
  summary: { relativeHalfWidth?: number } | undefined,
  precision: z.infer<typeof precisionSchema>,
) {
  const limit = precisionSchema.parse(precision).relativeHalfWidth;
  const halfWidth = summary?.relativeHalfWidth;
  if (
    typeof halfWidth !== "number" || !Number.isFinite(halfWidth) ||
    halfWidth < 0
  ) {
    throw new Error("Precision statistic is missing or nonfinite");
  }
  return halfWidth <= limit;
}

export function validateBenchmark(
  value: unknown,
  expected: BenchmarkExpectation,
) {
  const report = reportSchema.parse(value);
  if (report.parameters.protocol !== expected.protocol) {
    throw new Error("Wrong benchmark protocol");
  }
  for (
    const key of Object.keys(
      expected.parameters,
    ) as (keyof typeof expected.parameters)[]
  ) {
    if (report.parameters[key] !== expected.parameters[key]) {
      throw new Error(`Wrong benchmark parameter: ${key}`);
    }
  }
  if (
    report.results.length !== 1 ||
    report.results[0].adapter !== expected.adapter ||
    report.parameters.adapter !== expected.adapter
  ) {
    throw new Error("Expected exactly one result for the selected adapter");
  }
  const cells = report.results[0].protocols;
  if (Object.keys(cells).length !== 1 || !cells[expected.protocol]) {
    throw new Error("Unexpected benchmark cells");
  }
  const cell = cells[expected.protocol];
  if (cell.samples.length !== expected.parameters.repetitions) {
    throw new Error("Missing benchmark repetitions");
  }
  if (
    expected.parameters["busker-workers"] !== undefined &&
    (expected.adapter !== "busker" ||
      cell.samples.some((sample) => {
        const counts = sample["busker-workers-effective"];
        return !counts ||
          Object.values(counts).some((count) =>
            count !== expected.parameters["busker-workers"]
          );
      }))
  ) {
    throw new Error("Effective Busker worker count differs");
  }
  for (
    const environment of [
      report.environment,
      ...cell.samples.map((sample) => sample.environment),
    ]
  ) {
    if (
      environment["busker-checkout"] !== expected.checkout ||
      environment["busker-revision"] !== expected.revision ||
      environment["native-sha256"] !== expected.nativeSha256 ||
      resourceURL(environment["native-resource"]) !== expected.nativeResource ||
      resourceURL(environment["busker-resource"]) !== expected.sourceResource
    ) throw new Error("Benchmark source or native library does not match");
    if (expected.purpose === "measurement" && environment["busker-dirty?"]) {
      throw new Error("Cannot score dirty source");
    }
    if (environment["java-version"] !== report.environment["java-version"]) {
      throw new Error("JVM versions differ");
    }
  }
  for (const sample of cell.samples) {
    if (
      sample.response.protocol !==
        (expected.protocol === "tls-h2" ? "HTTP_2" : "HTTP_1_1")
    ) throw new Error("Negotiated protocol does not match");
    for (
      const flag of [
        "-Xms512m",
        "-Xmx2g",
        "-XX:ActiveProcessorCount=2",
        "-Dbusker.bench.local=true",
      ]
    ) {
      if (!sample.environment["jvm-options"].includes(flag)) {
        throw new Error(`Missing JVM flag: ${flag}`);
      }
    }
    if (
      sample.environment.processors !== 2 ||
      sample.environment["max-heap-bytes"] !== 2147483648
    ) throw new Error("Unexpected sample JVM limits");
  }
  const sampleRates = cell.samples.map((sample) =>
    sample["requests-per-second"]
  );
  const { rates, median } = sampleSummary(sampleRates);
  if (
    Math.abs(median - cell["requests-per-second"]) > 0.01 ||
    cell.range[0] !== rates[0] || cell.range[1] !== rates[rates.length - 1]
  ) {
    throw new Error("Benchmark summary does not match its samples");
  }
  const scale = rates[rates.length - 1];
  const total = rates.reduce((sum, rate) => sum + rate, 0);
  const mean = Number.isFinite(total)
    ? total / rates.length
    : rates.reduce((sum, rate) => sum + rate / scale, 0) / rates.length * scale;
  if (!Number.isFinite(mean)) throw new Error("Benchmark mean is not finite");
  return {
    kind: expected.purpose === "smoke"
      ? "smoke" as const
      : "benchmark" as const,
    protocol: expected.protocol,
    adapter: expected.adapter,
    diagnosticRate: mean,
    score: null as number | null,
    samples: sampleRates,
    javaVersion: report.environment["java-version"],
    h2loadVersion: report.environment["h2load-version"],
    parameters: expected.parameters,
  };
}
