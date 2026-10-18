# OpenCode2API integration (optional)

> 中文用户：部署命令、密钥位置、与 OpenCode Zen/Go 原生预置卡的取舍，见主 README 的「OpenCode2API 可选嵌入」一节。本文是完整的英文说明与故障排查。

This integration connects **[jasonxu114514/opencode2api](https://github.com/jasonxu114514/opencode2api)** to Agent2API **as a separate service**, using Agent2API's existing custom-provider API. It does **not** vendor or link upstream GPL-3.0-or-later Go sources into the Agent2API Rust/Tauri binary. The upstream image retains its own licensing and update lifecycle.

## What is integrated

| Feature | Integration behavior |
| --- | --- |
| OpenAI Chat Completions | Agent2API `/v1/chat/completions` → existing custom-provider forwarding → upstream `/v1/chat/completions` |
| Responses / Anthropic Messages | Agent2API's existing protocol bridge remains available. The sidecar also has native endpoints, but this integration does not bypass Agent2API's protocol bridge |
| Zen and Go API keys | Managed **inside OpenCode2API**, not copied into Agent2API |
| Multiple keys / retries / rotation / `sota`, `sweet` | Executed by the independent OpenCode2API service |
| Model discovery | Fetched from the upstream `/v1/models` in the background, retried with backoff (≈30 min) until the sidecar is ready; a catalogue you edited is never overwritten |
| Reasoning effort | Not forced by the seed; set it per model in Agent2API (or let the client send it) like any other provider |
| Request routing / API keys / reporting | Uses the existing Agent2API account routing, gateway authentication and reporting |
| Balance / quota | Not shown in Agent2API (upstream exposes it only behind its WebUI session); check the upstream WebUI |
| OpenCode2API WebUI | Separate upstream management interface bound to **localhost only** (default port 18081) |
| Standalone Windows/macOS Agent2API | Existing functionality unchanged; a manual `OpenCode2API` preset is available but the upstream Go binary is **not** bundled |

**Limitations:** Upstream WebUI, Zen/Go key management and metrics remain in the upstream application. The whole upstream key pool appears as **one** account in Agent2API, so per-key balance, cooldown and statistics are only visible upstream. This integration is not an in-process Rust port or a single-process desktop bundle.

## Docker Compose (opt-in)

One-time initialization from the Agent2API repository root:

```bash
node scripts/opencode2api-init.mjs
```

The script writes **untracked** secret-bearing files (both git- and docker-ignored, mode 0600 in a 0700 directory):

- `integrations/opencode2api/.secrets/config.json` — upstream seed: initial upstream WebUI administrator password, upstream key pools, local server API key. It is deliberately **outside `./data`**, which the Agent2API container mounts, so Agent2API never sees Zen/Go keys.
- `.env.opencode2api` — **only** `AGENT2API_OPENCODE2API_KEY`, the local server API key connecting Agent2API to OpenCode2API.

The script refuses to overwrite existing secrets or silently rotate a mismatched key, and only accepts keys matching `^[A-Za-z0-9._-]+$`. To seed Zen/Go credentials on the **first** run, set `OPENCODE2API_ZEN_KEYS` and/or `OPENCODE2API_GO_KEYS` in the environment before executing the initializer (comma- or newline-separated). Without either key, the initializer enables the upstream anonymous channel; `OPENCODE2API_ANONYMOUS=0` or `=1` overrides that.

Start the deployment:

```bash
docker compose -f docker-compose.yml -f docker-compose.opencode2api.yml up -d --build
```

No `--env-file` flag is needed (and passing only `--env-file .env.opencode2api` would make Compose ignore your normal `.env`). The overlay injects the bridge key with a service-level `env_file:`.

What the overlay adds:

1. `opencode2api-seed` — a one-shot container (same upstream image, no network, minimal capabilities) that copies the seed into the `opencode2api-state` volume **owned by the image's unprivileged `opencode2api` user, mode 0600**. Upstream's own entrypoint drops privileges before copying `CONFIG_SEED_PATH`, so it cannot read a 0600 file owned by your host user; this step avoids that restart loop. It does nothing once the volume already holds a config.
2. `opencode2api` — the sidecar itself, read-only root filesystem, no published API port.
3. `agent2api` gets `AGENT2API_OPENCODE2API_URL` / `AGENT2API_OPENCODE2API_KEY` and waits only for the sidecar container to **start**, not to become healthy: Agent2API keeps working even when the sidecar has no outbound network or no routable model.

The extra file is a **Compose override**; ordinary `docker compose up` continues to use Agent2API alone.

| Endpoint | Default address | Visibility |
| --- | --- | --- |
| Unified Agent2API gateway | `http://<host>:3065/v1` | The base Compose file publishes `0.0.0.0:3065` (all interfaces); keep gateway keys enabled |
| OpenCode2API upstream API | `http://opencode2api:8080/v1` | Docker network only; **no host port published** |
| OpenCode2API WebUI | `http://127.0.0.1:18081` | Host loopback only |

OpenCode2API is listed in Agent2API as a custom provider named **`OpenCode2API`** — the same name as the preset card, so it shows the preset icon and clicking that card adds accounts to it instead of creating a second provider. Registration is **idempotent** across restarts:

- The account name is set only when the account is first created; renaming it in the UI survives restarts.
- When `AGENT2API_OPENCODE2API_KEY` changes (key rotation), the existing account's credential is updated in place, and any other enabled account of this provider still holding an outdated key is disabled (it would only return 401).
- If you delete the provider in the UI while the overlay is still in use, it is registered again at the next start. To stop using the sidecar, start without the overlay file.

Confirm connectivity and the model catalogue:

```bash
curl http://127.0.0.1:3065/health
curl http://127.0.0.1:3065/v1/models -H 'Authorization: Bearer YOUR_AGENT2API_GATEWAY_KEY'
```

When Agent2API has gateway-key enforcement enabled (as it should on the LAN), use a key from **Agent2API's** gateway-Key page, not the upstream server key.

**Persistence:** Once the seed container fills the named state volume, later edits to the seed do **not** replace the persisted configuration. Change settings through the upstream WebUI. After the first successful start you can delete `integrations/opencode2api/.secrets/config.json` (recommended: it holds plaintext keys and the initial WebUI password); keep `.env.opencode2api`. Re-running the initializer afterwards is a no-op.

**Secret lifecycle:** Changing `server_keys` from the upstream WebUI also requires updating `AGENT2API_OPENCODE2API_KEY` in `.env.opencode2api` and recreating the Agent2API container (`docker compose … up -d`). Never put upstream Zen/Go keys in that file. To expose the WebUI remotely, configure an authenticated TLS reverse proxy separately; the supplied Compose configuration deliberately restricts it to localhost.

**Image version:** the overlay pins `ghcr.io/jasonxu114514/opencode2api:v1.4.1` by multi-arch digest. To upgrade after testing a newer release, set `OPENCODE2API_IMAGE=ghcr.io/jasonxu114514/opencode2api:<tag>` (optionally `@sha256:…`) in `.env`.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `OpenCode2API` provider has no models | `docker compose -f docker-compose.yml -f docker-compose.opencode2api.yml logs opencode2api`; then `docker compose … exec opencode2api wget -qO- http://127.0.0.1:8080/healthz` (no auth; `503` means no routable model yet, e.g. no outbound network or no valid Zen/Go key). Agent2API retries for about 30 minutes; afterwards use **Fetch upstream models** in model management |
| `opencode2api` keeps restarting | `docker compose … logs opencode2api-seed opencode2api`. A missing seed means the initializer was not run; a permission error on SELinux hosts may need the seed mount relabelled (`:z`) |
| Compose says `.env.opencode2api` not found | Run `node scripts/opencode2api-init.mjs` first |
| Requests return 401 from the sidecar | `.env.opencode2api` no longer matches the upstream `server_keys`; update it and recreate the Agent2API container |
| Provider not registered at all | Agent2API logs `[OpenCode2API]`; registration is skipped (and logged) while legacy data is waiting to be imported — restart after the import |

## Desktop / existing standalone service

Agent2API's **Add account → Preset API** selection includes `OpenCode2API`, defaulting to `http://127.0.0.1:8080/v1` (the upstream default listen address), for an independently running OpenCode2API executable:

1. Download a binary from the upstream [Releases](https://github.com/jasonxu114514/opencode2api/releases) and create `config.json` from its `config.example.json` (set `server_keys`, Zen/Go keys or `anonymous: true`, and a WebUI password).
2. Run `./opencode2api -config config.json`.
3. In Agent2API, pick the `OpenCode2API` preset and use one of the upstream **`server_keys`** as the account API Key, then fetch upstream models in model management.

This preset does not launch an upstream process. Setting `AGENT2API_OPENCODE2API_URL` and `AGENT2API_OPENCODE2API_KEY` for the desktop app also triggers the automatic registration described above. Docker deployments do not need the preset at all.

## Source and license

- Independent upstream application, container image, protocol and management UI: [jasonxu114514/opencode2api](https://github.com/jasonxu114514/opencode2api), [GPL-3.0-or-later LICENSE](https://github.com/jasonxu114514/opencode2api/blob/main/LICENSE).
- Integration glue is written specifically for Agent2API; no upstream Go source is copied.

This does not change the existing Agent2API release/tag workflow, main branch, or default image.
