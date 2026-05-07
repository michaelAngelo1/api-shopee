/**
 * Postinstall: npm on this system fails to fully extract some packages —
 * they end up with only README/LICENSE/package.json. This script detects
 * incomplete packages and re-downloads them from the registry.
 */
const { execSync } = require('child_process');
const { existsSync, readdirSync, readFileSync, mkdirSync, rmSync } = require('fs');
const { resolve, join, extname } = require('path');
const os = require('os');

const nm = resolve(__dirname, '../node_modules');

function isIncomplete(pkgDir, meta) {
  const checks = new Set();

  function collectEntryPoints(val) {
    if (typeof val === 'string' && !val.includes('*')) checks.add(val);
    else if (val && typeof val === 'object') Object.values(val).forEach(collectEntryPoints);
  }
  collectEntryPoints(meta.main);
  collectEntryPoints(meta.exports);

  // No entry points declared — assume OK
  if (checks.size === 0) return false;

  for (const ref of checks) {
    const full = resolve(pkgDir, ref);
    if (!existsSync(full) && !existsSync(full + '.js') && !existsSync(full + '.cjs')) {
      return true;
    }
  }

  // Also scan the main file for broken first-level internal requires
  // (e.g. protobufjs/index.js does `require('./src/index')` but src/ is missing)
  // Only flag if the main file IS at the package root (otherwise relative refs are fine)
  const mainRaw = meta.main;
  if (mainRaw && !mainRaw.includes('/')) {
    const mainFile = resolve(pkgDir, mainRaw.endsWith('.js') ? mainRaw : mainRaw + '.js');
    if (existsSync(mainFile)) {
      try {
        const src = readFileSync(mainFile, 'utf8').slice(0, 2000);
        const refs = [...src.matchAll(/require\(['"](\.[^'"]+)['"]\)/g)];
        for (const [, ref] of refs) {
          const full = resolve(pkgDir, ref);
          if (!existsSync(full) && !existsSync(full + '.js') && !existsSync(full + '.cjs') && !existsSync(full + '.json')) {
            return true;
          }
        }
      } catch (_) {}
    }
  }

  return false;
}

function getPackageDirs() {
  const dirs = [];
  for (const entry of readdirSync(nm)) {
    if (entry.startsWith('.')) continue;
    const full = join(nm, entry);
    if (entry.startsWith('@')) {
      try { for (const sub of readdirSync(full)) dirs.push(join(full, sub)); } catch (_) {}
    } else {
      dirs.push(full);
    }
  }
  return dirs;
}

function restore(pkgDir, name, version) {
  const tmp = resolve(os.tmpdir(), `fix-${name.replace('/', '-')}-${Date.now()}`);
  mkdirSync(tmp, { recursive: true });
  try {
    console.log(`[postinstall] Restoring ${name}@${version}...`);
    execSync(`npm pack ${name}@${version}`, { cwd: tmp, stdio: 'pipe' });
    execSync(`tar -xzf *.tgz`, { cwd: tmp, stdio: 'pipe' });
    execSync(`cp -r package/. "${pkgDir}/"`, { cwd: tmp, stdio: 'pipe' });
    console.log(`[postinstall] Restored ${name}@${version}`);
    return true;
  } catch (e) {
    console.error(`[postinstall] Failed to restore ${name}@${version}: ${e.message}`);
    return false;
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

let fixed = 0;
const failed = [];

for (const pkgDir of getPackageDirs()) {
  const pkgJson = join(pkgDir, 'package.json');
  if (!existsSync(pkgJson)) continue;
  let meta;
  try { meta = JSON.parse(readFileSync(pkgJson, 'utf8')); } catch (_) { continue; }
  if (!meta.name || !meta.version) continue;

  if (isIncomplete(pkgDir, meta)) {
    const ok = restore(pkgDir, meta.name, meta.version);
    if (ok) fixed++;
    else failed.push(`${meta.name}@${meta.version}`);
  }
}

if (fixed === 0 && failed.length === 0) {
  console.log('[postinstall] All packages OK.');
} else {
  if (fixed > 0) console.log(`[postinstall] Restored ${fixed} package(s).`);
  if (failed.length > 0) console.error(`[postinstall] Could not restore: ${failed.join(', ')}`);
}
