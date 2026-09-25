import { model } from "../../extensions/models/tempo.ts";
import type { ArtifactStore } from "../../extensions/models/_lib/artifacts.ts";
import type { OutputReference } from "../../extensions/models/_lib/inference.ts";

export class Store implements ArtifactStore {
  resources = new Map<string, Record<string, unknown>[]>();
  files = new Map<string, Uint8Array>();
  failArchive = false;

  async readResource(name: string, version?: number) {
    const versions = this.resources.get(name);
    return await Promise.resolve(
      structuredClone(versions?.[(version ?? versions.length) - 1] ?? null),
    );
  }
  async writeResource(
    spec: string,
    name: string,
    value: Record<string, unknown>,
  ) {
    if (!(spec in model.resources)) throw new Error(`Unknown resource ${spec}`);
    const parsed = model.resources[spec as keyof typeof model.resources].schema
      .parse(value);
    const versions = this.resources.get(name) ?? [];
    versions.push(structuredClone({ ...parsed }));
    this.resources.set(name, versions);
    return await Promise.resolve({ name, version: versions.length });
  }
  createFileWriter(spec: string, name: string) {
    if (!(spec in model.files)) throw new Error(`Unknown file spec ${spec}`);
    const save = (bytes: Uint8Array): OutputReference => {
      this.files.set(name, bytes);
      return { name, version: 1 };
    };
    return {
      writeText: (text: string) =>
        Promise.resolve(save(new TextEncoder().encode(text))),
      writeStream: async (stream: ReadableStream<Uint8Array>) => {
        if (this.failArchive) {
          throw new Deno.errors.NotFound("Archive unavailable");
        }
        return save(new Uint8Array(await new Response(stream).arrayBuffer()));
      },
    };
  }
}
