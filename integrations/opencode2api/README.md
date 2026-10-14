# OpenCode2API integration (optional)

This integration connects **[jasonxu114514/opencode2api](https://github.com/jasonxu114514/opencode2api)** to Agent2API **as a separate service**, using Agent2API's existing custom-provider API. It does **not** vendor or link upstream GPL-3.0-or-later Go sources into the Agent2API Rust/Tauri binary. The upstream image retains its own licensing and update lifecycle.

## What is integrated

| Feature | Integration behavior |
| --- | --- |
| OpenAI Chat Completions | Agent2API `/v1/chat/completions` → existing custom-provider forwarding → upstream `/v1/chat/completions` |
| Responses / Anthropic Messages | Agent2API's existing protocol bridge remains available. The sidecar also has native endpoints, but this integration does not bypass Agent2API's protocol bridge |
| Zen and Go API keys | Managed **inside OpenCode2API**, not copied into Agent2API |
| Multiple keys / retries / rotation / `sota`, `sweet` | Executed by the independent OpenCode2API service |
| Model discovery | The registered custom provider initially advertises `sota`; a background fetch from the upstream `/v1/models` replaces that provisional entry when successful |
| Request routing / API keys / reporting | Uses the existing Agent2API account routing, gateway authentication and reporting |
| OpenCode2API WebUI | Separate upstream management interface bound to **localhost only** (default port 18081) |
| Standalone Windows/macOS Agent2API | Existing functionality unchanged; a manual local-sidecar preset is available but the upstream Go binary is **not** bundled |

**Limitations:** Upstream WebUI, Zen/Go key management and metrics remain in the upstream application. This integration is not an in-process Rust port or a single-process desktop bundle. Sidecar restart, credential migration, model changes and monitoring remain distinct from native Agent2API providers.

## Docker Compose (opt-in)

One-time initialization from the Agent2API repository root:

```bash
node scripts/opencode2api-init.mjs
```

The script writes **untracked** secret-bearing files:

- `data/opencode2api/config.json` — upstream config, initial upstream WebUI administrator password, upstream key pools, local server API key.
- `.env.opencode2api` — **only** the local server API key connecting Agent2API to OpenCode2API.

Both locations are ignored by Git; neither should be committed. The script refuses to overwrite existing secrets or silently rotate a mismatched key. To seed Zen/Go credentials on the **first** run, set `OPENCODE2API_ZEN_KEYS` and/or `OPENCODE2API_GO_KEYS` in the environment before executing the initializer. Comma- or newline-separated values are supported. Without either key, the initializer enables the upstream anonymous channel by default; you can instead set `OPENCODE2API_ANONYMOUS=0`.

To start the two-container deployment:

```bash
docker compose --env-file .env.opencode2api \
  -f docker-compose.yml -f docker-compose.opencode2api.yml \
  up -d --build
```

The extra file is a **Compose override**; ordinary `docker compose up` continues to use Agent2API alone.

| Endpoint | Default address | Visibility |
| --- | --- | --- |
| Unified Agent2API gateway | `http://127.0.0.1:3065/v1` | Follows existing Agent2API host port mapping |
| OpenCode2API upstream API | `http://opencode2api:8080/v1` | Docker network only; **no host port published** |
| OpenCode2API WebUI | `http://127.0.0.1:18081` | Host loopback only |

OpenCode2API is listed as a regular custom provider in Agent2API with its own account. The registration is **idempotent** across application restarts when the same base URL and server key are supplied. The upstream server key is not sent to Zen or Go; only the upstream OpenCode2API service receives the key.

Confirm connectivity and the model catalogue:

```bash
curl http://127.0.0.1:3065/health
curl http://127.0.0.1:3065/v1/models -H 'Authorization: Bearer YOUR_AGENT2API_GATEWAY_KEY'
```

When Agent2API has gateway-key enforcement enabled (as it should on the LAN), use a key from **Agent2API's** gateway-Key page, not the upstream server key.

If the provisional `sota` catalogue remains, open Agent2API's model management for this custom provider and use its normal **fetch upstream models** action. If you want other manual mappings, configure them there.

**Persistence:** Once the upstream container seeds its named state volume, later edits to `data/opencode2api/config.json` do **not** automatically replace the persisted configuration. Change settings through the upstream management UI or explicitly migrate its config; do not delete the named volume to re-import because that would erase upstream settings.

**Secret lifecycle:** Changing `server_keys` from the upstream WebUI also requires updating `.env.opencode2api` and restarting the Agent2API service so its custom account uses the new key. Never put upstream Zen/Go keys in that `.env` file. WebUI password is generated in `data/opencode2api/config.json` on first init; change it through upstream WebUI. To expose WebUI remotely, configure an authenticated TLS reverse proxy separately; the supplied Compose configuration deliberately restricts it to localhost.

## Desktop / existing standalone service

Agent2API's **Add account → Preset API** selection includes:

- `OpenCode2API (本机)` with default `http://127.0.0.1:8080/v1` (for an independently running local opencode2api executable).
- `OpenCode2API (Docker)` with default `http://opencode2api:8080/v1` (Docker-internal DNS; accessible only from containers on the same Compose network).

Use the upstream **`server_keys`** value as the **custom account API Key**, then fetch upstream models using the normal model-management UI. These presets do not launch an upstream process.

## Source and license

- Independent upstream application, container image, protocol and management UI: [jasonxu114514/opencode2api](https://github.com/jasonxu114514/opencode2api), [GPL-3.0-or-later LICENSE](https://github.com/jasonxu114514/opencode2api/blob/main/LICENSE).
- Integration glue is written specifically for Agent2API; no upstream Go source is copied.
- The image tag defaults to `latest`. For reproducible deployments, set `OPENCODE2API_VERSION` to a tested release tag.

This does not change the existing Agent2API release/tag workflow, main branch, or default image.
