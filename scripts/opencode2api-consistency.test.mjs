// Cross-file invariants of the optional OpenCode2API integration.
// Run with: node --test scripts/opencode2api-consistency.test.mjs
import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const read = (path) => readFileSync(join(root, path), 'utf8')

test('preset card name equals the provider name the Rust bridge registers', () => {
  const bridge = read('desktop-tauri/src-tauri/server/src/server/core/opencode2api_bridge.rs')
  const rustName = bridge.match(/pub const NAME: &str = "([^"]+)";/)?.[1]
  assert.ok(rustName, 'NAME constant not found in opencode2api_bridge.rs')
  const presets = read('desktop-tauri/ui/preset-providers.js')
  const card = presets.match(/key: 'opencode2api-local', name: '([^']+)'/)?.[1]
  assert.equal(card, rustName)
})

test('Compose overlay keeps the sidecar optional and secrets out of ./data', () => {
  const overlay = read('docker-compose.opencode2api.yml')
  assert.doesNotMatch(overlay, /service_healthy/)
  assert.doesNotMatch(overlay, /\.\/data\//)
  assert.match(overlay, /condition: service_started/)
  assert.match(overlay, /condition: service_completed_successfully/)
  assert.match(overlay, /@sha256:[0-9a-f]{64}/)
  assert.doesNotMatch(overlay, /opencode2api:\$\{[^}]*:-latest\}/)
})

test('secret locations are ignored by git and docker', () => {
  const gitignore = read('.gitignore')
  assert.match(gitignore, /^\.env\.opencode2api$/m)
  assert.match(gitignore, /^integrations\/opencode2api\/\.secrets\/$/m)
  const dockerignore = read('.dockerignore')
  assert.match(dockerignore, /^\.env\.\*$/m)
  assert.match(dockerignore, /^integrations$/m)
})
