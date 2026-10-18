// Integration tests for the OpenCode2API optional initializer.
// Run with: node --test scripts/opencode2api-init.test.mjs
// Tests stage a copy of the script in a temporary repository root so no
// developer credentials or live Agent2API data are ever accessed.
import test from 'node:test'
import assert from 'node:assert/strict'
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, unlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'

const source = join(dirname(fileURLToPath(import.meta.url)), 'opencode2api-init.mjs')
const posix = process.platform !== 'win32'

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), 'agent2api-oc2api-'))
  t.after(() => rmSync(root, { recursive: true, force: true }))
  mkdirSync(join(root, 'scripts'))
  const script = join(root, 'scripts', 'opencode2api-init.mjs')
  copyFileSync(source, script)
  const secretsDir = join(root, 'integrations', 'opencode2api', '.secrets')
  const configPath = join(secretsDir, 'config.json')
  const envPath = join(root, '.env.opencode2api')
  const run = (extra = {}) => {
    const env = { ...process.env }
    // Avoid inheriting real credentials from developer CI host.
    delete env.OPENCODE2API_GO_KEYS
    delete env.OPENCODE2API_ZEN_KEYS
    delete env.OPENCODE2API_ANONYMOUS
    return spawnSync(process.execPath, [script], {
      cwd: root, env: { ...env, ...extra }, encoding: 'utf8',
    })
  }
  const writeSeed = (cfg) => {
    mkdirSync(secretsDir, { recursive: true })
    writeFileSync(configPath, typeof cfg === 'string' ? cfg : JSON.stringify(cfg))
  }
  return { root, run, secretsDir, configPath, envPath, writeSeed }
}

test('anonymous first-run config and local bridge key are generated securely', (t) => {
  const { run, configPath, envPath, root } = fixture(t)
  const result = run()
  assert.equal(result.status, 0, result.stderr)
  assert.ok(existsSync(configPath))
  assert.ok(existsSync(envPath))
  // The seed must never land under ./data, which the Agent2API container mounts.
  assert.ok(!existsSync(join(root, 'data')))
  const cfg = JSON.parse(readFileSync(configPath, 'utf8'))
  assert.equal(cfg.listen, '0.0.0.0:8080')
  assert.equal(cfg.anonymous, true)
  assert.equal(cfg.zen_keys.length, 0)
  assert.equal(cfg.go_keys.length, 0)
  assert.ok(cfg.server_keys[0].startsWith('a2a-'))
  assert.ok(cfg.webui.password.length >= 32)
  // Upstream defaults are left to upstream: no forced effort, no stale rotation list.
  assert.equal(cfg.reasoning, undefined)
  assert.equal(cfg.rotation, undefined)
  const content = readFileSync(envPath, 'utf8')
  assert.ok(content.includes('AGENT2API_OPENCODE2API_KEY=' + cfg.server_keys[0]))
  assert.ok(!content.includes(cfg.webui.password))
  assert.ok(!content.includes('ZEN_KEYS'))
})

test('secret files are private to the invoking user', { skip: !posix && 'POSIX modes only' }, (t) => {
  const { run, secretsDir, configPath, envPath } = fixture(t)
  assert.equal(run().status, 0)
  assert.equal(statSync(secretsDir).mode & 0o777, 0o700)
  assert.equal(statSync(configPath).mode & 0o777, 0o600)
  assert.equal(statSync(envPath).mode & 0o777, 0o600)
})

test('repeated bootstrap preserves credentials and key file byte-for-byte', (t) => {
  const { run, configPath, envPath } = fixture(t)
  assert.equal(run().status, 0)
  const a = readFileSync(configPath, 'utf8')
  const b = readFileSync(envPath, 'utf8')
  assert.equal(run({ OPENCODE2API_GO_KEYS: 'unused-later' }).status, 0)
  assert.equal(readFileSync(configPath, 'utf8'), a)
  assert.equal(readFileSync(envPath, 'utf8'), b)
})

test('explicit upstream keys turn off anonymous and are not copied to bridge .env', (t) => {
  const { run, configPath, envPath } = fixture(t)
  const result = run({
    OPENCODE2API_ZEN_KEYS: 'test-zen-one,test-zen-two',
    OPENCODE2API_GO_KEYS: 'test-go-one',
  })
  assert.equal(result.status, 0, result.stderr)
  const cfg = JSON.parse(readFileSync(configPath, 'utf8'))
  assert.equal(cfg.anonymous, false)
  assert.deepEqual(cfg.zen_keys, ['test-zen-one', 'test-zen-two'])
  assert.deepEqual(cfg.go_keys, ['test-go-one'])
  assert.ok(!readFileSync(envPath, 'utf8').includes('test-zen-one'))
  assert.ok(!readFileSync(envPath, 'utf8').includes('test-go-one'))
})

test('OPENCODE2API_ANONYMOUS=0/1 overrides the default', (t) => {
  const off = fixture(t)
  assert.equal(off.run({ OPENCODE2API_ANONYMOUS: '0' }).status, 0)
  assert.equal(JSON.parse(readFileSync(off.configPath, 'utf8')).anonymous, false)
  const on = fixture(t)
  assert.equal(on.run({ OPENCODE2API_ANONYMOUS: '1', OPENCODE2API_ZEN_KEYS: 'k' }).status, 0)
  assert.equal(JSON.parse(readFileSync(on.configPath, 'utf8')).anonymous, true)
  const bad = fixture(t)
  const result = bad.run({ OPENCODE2API_ANONYMOUS: 'yes' })
  assert.notEqual(result.status, 0)
  assert.match(result.stderr, /must be 0 or 1/)
})

test('mismatched shared keys fail closed instead of rotating silently', (t) => {
  const { run, envPath } = fixture(t)
  assert.equal(run().status, 0)
  writeFileSync(envPath, 'AGENT2API_OPENCODE2API_KEY=wrong\n')
  const result = run()
  assert.notEqual(result.status, 0)
  assert.match(result.stderr, /does not match/)
})

test('server key with characters outside [A-Za-z0-9._-] is rejected (no env injection)', (t) => {
  const { run, writeSeed, envPath } = fixture(t)
  writeSeed({ server_keys: ['abc\nAGENT2API_ALLOW_NO_KEY=1'] })
  const result = run()
  assert.notEqual(result.status, 0)
  assert.match(result.stderr, /may only contain/)
  assert.ok(!existsSync(envPath))
})

test('corrupt seed JSON gives a friendly error instead of a stack trace', (t) => {
  const { run, writeSeed } = fixture(t)
  writeSeed('{ "server_keys": [')
  const result = run()
  assert.notEqual(result.status, 0)
  assert.match(result.stderr, /is not valid JSON/)
  assert.doesNotMatch(result.stderr, /at .*opencode2api-init\.mjs:\d+/)
})

test('seed removed after first start is accepted while the bridge key is kept', (t) => {
  const { run, configPath, envPath } = fixture(t)
  assert.equal(run().status, 0)
  const before = readFileSync(envPath, 'utf8')
  unlinkSync(configPath)
  const result = run()
  assert.equal(result.status, 0, result.stderr)
  assert.match(result.stdout, /keeping existing/)
  assert.ok(!existsSync(configPath))
  assert.equal(readFileSync(envPath, 'utf8'), before)
})
