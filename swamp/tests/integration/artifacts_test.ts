import { deepStrictEqual, match, rejects } from "node:assert/strict";
import {
  archiveDirectory,
  type ArtifactStore,
  logExcerpt,
} from "../../extensions/models/_lib/artifacts.ts";
import { Store } from "../fixtures/store.ts";

Deno.test("archived trees normalize Swamp file handles, omit TLS private keys, and reject links", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-artifacts-" });
  try {
    await Deno.writeTextFile(`${root}/output.log`, "result");
    await Deno.writeTextFile(`${root}/server.key`, "private");
    await Deno.writeTextFile(`${root}/server.p12`, "private");
    const base = new Store();
    const store: ArtifactStore = {
      readResource: base.readResource.bind(base),
      writeResource: base.writeResource.bind(base),
      createFileWriter: (spec, name) => ({
        ...base.createFileWriter(spec, name),
        writeStream: async (stream) => ({
          ...await base.createFileWriter(spec, name).writeStream(stream),
          specName: spec,
          kind: "file",
          dataId: "id",
          size: 6,
          tags: {},
          metadata: {},
        }),
      }),
    };
    const saved = await archiveDirectory(store, "profileFile", "fixture", root);
    deepStrictEqual(saved.files, [{
      path: "output.log",
      reference: { name: "fixture-file-1", version: 1 },
    }]);
    deepStrictEqual(saved.omitted.sort((a, b) => a.localeCompare(b)), [
      "server.key",
      "server.p12",
    ]);
    deepStrictEqual(Object.keys(saved.handles[0]).includes("dataId"), true);
    await Deno.symlink(`${root}/output.log`, `${root}/link`);
    await rejects(
      () => archiveDirectory(store, "profileFile", "other", root),
      /symlinks/,
    );
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});
Deno.test("failure excerpts retain complete small logs and bounded head/tail text for large logs", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-excerpt-" });
  try {
    const path = `${root}/log`;
    await Deno.writeTextFile(path, "first failure");
    deepStrictEqual(await logExcerpt(path), {
      text: "first failure",
      totalBytes: 13,
      truncated: false,
    });
    await Deno.writeTextFile(path, "start" + "x".repeat(20000) + "end");
    const result = await logExcerpt(path);
    deepStrictEqual([result.totalBytes, result.truncated], [20008, true]);
    match(result.text, /^start/);
    match(result.text, /omitted; full log is archived/);
    match(result.text, /end$/);
    deepStrictEqual(result.text.length < 16500, true);
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});
