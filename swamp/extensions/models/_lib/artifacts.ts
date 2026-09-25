import type { OutputReference, ResourceStore } from "./inference.ts";

export interface ArtifactStore extends ResourceStore {
  createFileWriter(spec: string, name: string): {
    writeText(text: string): Promise<OutputReference>;
    writeStream(stream: ReadableStream<Uint8Array>): Promise<OutputReference>;
  };
}

export async function archiveFile(
  store: ArtifactStore,
  spec: string,
  name: string,
  path: string,
  allowMissing = false,
) {
  let file: Deno.FsFile;
  try {
    file = await Deno.open(path, { read: true });
  } catch (error) {
    if (allowMissing && error instanceof Deno.errors.NotFound) return null;
    throw error;
  }
  const failures: unknown[] = [];
  let handle: OutputReference | null = null;
  try {
    handle = await store.createFileWriter(spec, name).writeStream(
      file.readable,
    );
  } catch (error) {
    failures.push(error);
  }
  try {
    file.close();
  } catch (error) {
    if (!(error instanceof Deno.errors.BadResource)) failures.push(error);
  }
  if (failures.length) {
    throw new AggregateError(
      failures,
      `Cannot archive command output: ${failures.map(String).join("; ")}`,
    );
  }
  return handle;
}

export async function archiveLogs(
  store: ArtifactStore,
  spec: string,
  prefix: string,
  paths: { stdoutPath: string; stderrPath: string },
  allowMissing: boolean,
) {
  const handles: OutputReference[] = [];
  for (
    const [kind, path] of [["trace", paths.stdoutPath], [
      "stderr",
      paths.stderrPath,
    ]]
  ) {
    const handle = await archiveFile(
      store,
      spec,
      `${prefix}-${kind}`,
      path,
      allowMissing,
    );
    if (handle) handles.push(handle);
  }
  return handles;
}

export async function archiveDirectory(
  store: ArtifactStore,
  spec: string,
  prefix: string,
  directory: string,
) {
  const files: { path: string; reference: OutputReference }[] = [];
  const handles: OutputReference[] = [];
  const omitted: string[] = [];
  async function visit(path: string, relative: string) {
    for await (const entry of Deno.readDir(path)) {
      const label = relative ? `${relative}/${entry.name}` : entry.name;
      if (entry.isSymlink) {
        throw new Error("Artifacts must not contain symlinks");
      }
      if (entry.isDirectory) await visit(`${path}/${entry.name}`, label);
      else if (["server.key", "server.p12"].includes(entry.name)) {
        omitted.push(label);
      } else if (entry.isFile) {
        const reference = await archiveFile(
          store,
          spec,
          `${prefix}-file-${files.length + 1}`,
          `${path}/${entry.name}`,
        );
        if (reference) {
          handles.push(reference);
          files.push({
            path: label,
            reference: { name: reference.name, version: reference.version },
          });
        }
      } else throw new Error("Unexpected artifact type");
    }
  }
  await visit(directory, "");
  return { files, omitted, handles };
}

export async function logExcerpt(path: string) {
  const file = await Deno.open(path, { read: true });
  try {
    const totalBytes = (await file.stat()).size;
    const limit = 16384;
    const truncated = totalBytes > limit;
    const chunks = [];
    for (const start of truncated ? [0, totalBytes - limit / 2] : [0]) {
      await file.seek(start, Deno.SeekMode.Start);
      const bytes = new Uint8Array(truncated ? limit / 2 : totalBytes);
      let filled = 0;
      while (filled < bytes.length) {
        const count = await file.read(bytes.subarray(filled));
        if (count === null) break;
        filled += count;
      }
      chunks.push(new TextDecoder().decode(bytes.subarray(0, filled)));
    }
    return {
      text: chunks.join("\n[... omitted; full log is archived ...]\n"),
      totalBytes,
      truncated,
    };
  } finally {
    file.close();
  }
}
