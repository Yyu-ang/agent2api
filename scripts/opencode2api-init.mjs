#!/usr/bin/env node
/**
 * Idempotent, dependency-free bootstrap for the optional OpenCode2API sidecar.
 *
 * No upstream Go source is copied into Agent2API. The independent upstream
 * image retains its own GPL-3.0-or-later license.
 *
 * Supply optional upstream credentials via environment variables (not CLI
 * flags, which would leak into the process list):
 *   OPENCODE2API_ZEN_KEYS=sk-...
 *   OPENCODE2API_GO_KEYS=sk-...
 *   OPENCODE2API_ANONYMOUS=1   (or 0; default: on only when no key is given)
 *
 * Files written (both git- and docker-ignored):
 *   integrations/opencode2api/.secrets/config.json  upstream seed (Zen/Go keys,
 *       WebUI password). Lives OUTSIDE ./data so the Agent2API container never
 *       sees it; only the one-shot seed container mounts this directory.
 *   .env.opencode2api  only the local bridge key, injected into the Agent2API
 *       container through the service-level `env_file:` of the Compose overlay.
 */
import { randomBytes } from 'node:crypto'
import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, join, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const secretsDir = join(root, 'integrations', 'opencode2api', '.secrets')
const configPath = join(secretsDir, 'config.json')
const envPath = join(root, '.env.opencode2api')
const ENV_KEY = 'AGENT2API_OPENCODE2API_KEY'
// The key ends up in an env file and in an HTTP Authorization header. Anything
// outside this charset (newlines above all) could inject extra variables into
// the env file or break the header, so refuse it instead of quoting it.
const KEY_PATTERN = /^[A-Za-z0-9._-]+$/
const START = 'docker compose -f docker-compose.yml -f docker-compose.opencode2api.yml up -d --build'

const fail = (message) => {
  console.error(`opencode2api-init: ${message}`)
  process.exit(1)
}
const show = (path) => relative(root, path) || path
const secret = () => randomBytes(32).toString('hex')
const keys = (value) => (value || '').split(/[\n,]+/).map((key) => key.trim()).filter(Boolean)

function anonymousSetting(zenKeys, goKeys) {
  const raw = (process.env.OPENCODE2API_ANONYMOUS || '').trim()
  if (raw === '1') return true
  if (raw === '0') return false
  if (raw) fail(`OPENCODE2API_ANONYMOUS must be 0 or 1, got "${raw}".`)
  return !zenKeys.length && !goKeys.length
}

function readEnvKey() {
  const current = readFileSync(envPath, 'utf8')
  const match = current.match(new RegExp(`^${ENV_KEY}=(.*)$`, 'm'))
  return match ? match[1].trim() : null
}

const zenKeys = keys(process.env.OPENCODE2API_ZEN_KEYS)
const goKeys = keys(process.env.OPENCODE2API_GO_KEYS)

if (!existsSync(configPath) && existsSync(envPath)) {
  // The README recommends deleting the plaintext seed once the sidecar has
  // imported it into its state volume. That is a normal state, not an error.
  const key = readEnvKey()
  if (!key || !KEY_PATTERN.test(key)) {
    fail(`${show(envPath)} exists but has no valid ${ENV_KEY}. Delete it to re-initialize.`)
  }
  console.log(`Seed ${show(configPath)} not present (already imported and removed?); keeping existing ${show(envPath)}.`)
  console.log(`Start with:\n  ${START}`)
  process.exit(0)
}

let config
if (existsSync(configPath)) {
  try {
    config = JSON.parse(readFileSync(configPath, 'utf8'))
  } catch (error) {
    fail(`${show(configPath)} is not valid JSON (${error.message}). Fix it by hand or delete it and ${show(envPath)} to re-initialize.`)
  }
  console.log('Existing OpenCode2API seed config retained; no upstream keys/passwords overwritten.')
} else {
  mkdirSync(secretsDir, { recursive: true, mode: 0o700 })
  try { chmodSync(secretsDir, 0o700) } catch { /* non-POSIX filesystems */ }
  // Only fields that differ from upstream defaults are written; upstream fills
  // in endpoints, retry, model refresh and the sota/sweet rotation groups, so
  // the seed does not go stale when upstream changes those defaults. Reasoning
  // effort is deliberately left unset: Agent2API's per-model binding (or the
  // client) decides it, as with every other provider.
  config = {
    listen: '0.0.0.0:8080',
    server_keys: [`a2a-${secret()}`],
    zen_keys: zenKeys,
    go_keys: goKeys,
    anonymous: anonymousSetting(zenKeys, goKeys),
    proxies: ['direct'],
    webui: {
      enabled: true,
      listen: '0.0.0.0:8081',
      username: 'admin',
      password: secret(),
      session_ttl_minutes: 720,
    },
    prefer: 'go',
  }
  writeFileSync(configPath, JSON.stringify(config, null, 2) + '\n', { encoding: 'utf8', mode: 0o600, flag: 'wx' })
  console.log(`Created private upstream seed ${show(configPath)} (Zen/Go credentials, if supplied, stored locally).`)
}

const apiKey = typeof config?.server_keys?.[0] === 'string' ? config.server_keys[0].trim() : ''
if (!apiKey) {
  fail('Existing upstream config has no usable server_keys[0]; refusing to generate a broken bridge.')
}
if (!KEY_PATTERN.test(apiKey)) {
  fail('server_keys[0] may only contain letters, digits, ".", "_" and "-". Change it in the seed (and the upstream WebUI) first.')
}
// Only the local gateway key goes into the env file. Zen and Go account keys
// are NEVER written into the environment file or the Agent2API database.
const envContent = `# Generated by scripts/opencode2api-init.mjs. Do not commit.\n${ENV_KEY}=${apiKey}\n`
if (existsSync(envPath)) {
  if (readEnvKey() !== apiKey) {
    fail(`Existing ${show(envPath)} does not match server_keys[0]. Refusing to rotate credentials silently.`)
  }
} else {
  writeFileSync(envPath, envContent, { encoding: 'utf8', mode: 0o600, flag: 'wx' })
}
console.log('Private sidecar connection configuration is ready.')
console.log(`Start with:\n  ${START}`)
console.log('Models and account will be registered automatically in Agent2API at first gateway start.')
console.log(`After the first successful start you may delete ${show(configPath)}; the sidecar keeps its own copy.`)
console.log('Management UI (localhost only): http://127.0.0.1:18081')
