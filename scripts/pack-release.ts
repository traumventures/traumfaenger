// Packs @traumfaenger/api and @traumfaenger/client as npm tarballs for a GitHub
// release, so consumers install them from the release URL without a registry
// or a token:
//
//   "@traumfaenger/client": "https://github.com/traumventures/traumfaenger/releases/download/v0.1.0/traumfaenger-client-0.1.0.tgz"
//
// client's `workspace:*` dependency on api becomes the URL of api's tarball in
// the same release; one URL in the consumer pulls in both.
//
// Usage: bun scripts/pack-release.ts v0.1.0 [out-dir]
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";

const PACKAGES = ["api", "client"] as const;
const root = resolve(import.meta.dir, "..");
const repository = process.env.GITHUB_REPOSITORY ?? "traumventures/traumfaenger";

const tag = process.argv[2] ?? "";
const version = /^v(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)$/.exec(tag)?.[1];
if (!version) throw new Error(`expected a tag like v0.1.0, got "${tag}"`);
const outDir = resolve(process.argv[3] ?? join(root, "dist"));

// The tag, both npm packages and the Rust workspace carry one version.
const cargoVersion = /^\[workspace\.package\][^[]*?^version\s*=\s*"([^"]+)"/ms.exec(
  readFileSync(join(root, "Cargo.toml"), "utf8"),
)?.[1];
if (cargoVersion !== version) throw new Error(`Cargo.toml has version ${cargoVersion}, tag says ${version}`);

const tarball = (name: string) => `traumfaenger-${name}-${version}.tgz`;
const releaseUrl = (name: string) => `https://github.com/${repository}/releases/download/${tag}/${tarball(name)}`;

mkdirSync(outDir, { recursive: true });
const stage = mkdtempSync(join(tmpdir(), "traumfaenger-pack-"));
try {
  for (const name of PACKAGES) {
    const dir = join(stage, name);
    cpSync(join(root, "packages", name), dir, {
      recursive: true,
      filter: (src) => !src.includes("node_modules") && !src.endsWith(".test.ts"),
    });
    const manifestPath = join(dir, "package.json");
    const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    if (manifest.version !== version) {
      throw new Error(`packages/${name}/package.json has version ${manifest.version}, tag says ${version}`);
    }
    for (const [dep, spec] of Object.entries<string>(manifest.dependencies ?? {})) {
      if (!spec.startsWith("workspace:")) continue;
      const sibling = dep.replace(/^@traumfaenger\//, "");
      if (!PACKAGES.includes(sibling as (typeof PACKAGES)[number])) throw new Error(`unknown workspace dependency ${dep}`);
      manifest.dependencies[dep] = releaseUrl(sibling);
    }
    manifest.repository = { type: "git", url: `git+https://github.com/${repository}.git`, directory: `packages/${name}` };
    writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 4)}\n`);
    const pack = Bun.spawnSync(["bun", "pm", "pack", "--quiet", "--destination", outDir], { cwd: dir, stderr: "inherit" });
    if (pack.exitCode !== 0) throw new Error(`bun pm pack failed for ${name}`);
    const produced = basename(pack.stdout.toString().trim().split("\n").pop() ?? "");
    if (produced !== tarball(name)) throw new Error(`bun pm pack wrote ${produced}, expected ${tarball(name)}`);
    console.log(join(outDir, produced));
  }
} finally {
  rmSync(stage, { recursive: true, force: true });
}
