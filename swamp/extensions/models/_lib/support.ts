import { createHash } from "node:crypto";
import { join } from "node:path";

const runtimePaths = [
  ".swamp.yaml",
  "extensions",
  "models",
  "workflows",
  "prompts",
  "pi",
  "clojure",
];

export interface SupportManifest {
  sha256: string;
  files: { path: string; sha256: string }[];
}

export async function supportManifest(
  directory: string,
): Promise<SupportManifest> {
  const files: SupportManifest["files"] = [];
  async function visit(path: string) {
    const full = join(directory, path);
    const stat = await Deno.lstat(full);
    if (stat.isSymlink) {
      throw new Error(`Support symlink is not permitted: ${path}`);
    }
    if (stat.isDirectory) {
      for await (const entry of Deno.readDir(full)) {
        await visit(`${path}/${entry.name}`);
      }
    } else if (stat.isFile) {
      const bytes = await Deno.readFile(full);
      files.push({
        path,
        sha256: createHash("sha256").update(bytes).digest("hex"),
      });
    } else throw new Error(`Unsupported support file: ${path}`);
  }
  for (const path of runtimePaths) await visit(path);
  files.sort((a, b) => a.path.localeCompare(b.path));
  const sha256 = createHash("sha256").update(JSON.stringify(files)).digest(
    "hex",
  );
  return { sha256, files };
}

export async function verifyReviewedSupport(
  repository: string,
  revision: string,
  directory: string,
  manifest: SupportManifest,
) {
  if (!/^[0-9a-f]{40}$/.test(revision)) {
    throw new Error("Reviewed support needs a full Git commit hash");
  }
  const git = async (...args: string[]) => {
    const result = await new Deno.Command("git", { cwd: repository, args })
      .output();
    if (!result.success) {
      throw new Error(
        `Cannot inspect reviewed support commit: ${
          new TextDecoder().decode(result.stderr)
        }`,
      );
    }
    return result.stdout;
  };
  const type = new TextDecoder().decode(await git("cat-file", "-t", revision))
    .trim();
  if (type !== "commit") {
    throw new Error("Reviewed support revision is not a commit");
  }
  const objectFormat = new TextDecoder().decode(
    await git("rev-parse", "--show-object-format"),
  ).trim();
  if (objectFormat !== "sha1") {
    throw new Error(
      "Reviewed support requires the repository's SHA-1 Git object format",
    );
  }
  const tree = await git(
    "ls-tree",
    "-r",
    "-z",
    "--full-tree",
    revision,
    "--",
    ...runtimePaths.map((path) => `swamp/${path}`),
  );
  const blobs = new Map<string, string>();
  for (
    const entry of new TextDecoder().decode(tree).split("\0").filter(Boolean)
  ) {
    const match = /^(100644|100755) blob ([0-9a-f]{40})\tswamp\/(.+)$/.exec(
      entry,
    );
    if (!match || blobs.has(match[3])) {
      throw new Error("Reviewed support contains an unexpected Git entry");
    }
    blobs.set(match[3], match[2]);
  }
  if (blobs.size !== manifest.files.length) {
    throw new Error(
      `Support file set differs from reviewed Git commit: missing ${
        manifest.files.filter((file) => !blobs.has(file.path)).map((file) =>
          file.path
        ).join(", ")
      }; unexpected ${
        [...blobs.keys()].filter((path) =>
          !manifest.files.some((file) => file.path === path)
        ).join(", ")
      }`,
    );
  }
  for (const file of manifest.files) {
    const bytes = await Deno.readFile(join(directory, file.path));
    const oid = createHash("sha1").update(`blob ${bytes.length}\0`).update(
      bytes,
    )
      .digest("hex");
    if (blobs.get(file.path) !== oid) {
      throw new Error(
        `Support file differs from reviewed Git commit: ${file.path}`,
      );
    }
  }
}

export async function verifySupport(
  directory: string,
  expected: SupportManifest,
) {
  const actual = await supportManifest(directory);
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      "Support code changed; stop and inspect this run before any further effect",
    );
  }
}
