# KIMI Membership Relay

This internal-only relay lets an OpenAI API-key account in Sub2API use a Kimi
Code membership OAuth session. It refreshes the short-lived access token,
forces every Chat Completions request to model `k3`, and translates the native
KIMI effort levels (`low`, `high`, `max`) to `thinking.effort`.

The relay intentionally exposes only `/health`, `/v1/models`, and
`/v1/chat/completions`. A random internal Bearer secret protects the two `/v1`
routes. The deployment overlay publishes no host port and forces all KIMI
upstream traffic through the configured proxy.

Build a static Linux binary with:

```sh
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
  -o bin/kimi-membership-relay ./cmd/kimi-membership-relay
```

See `deploy/docker-compose.kimi-membership.yml` for the required file mounts
and runtime limits. Never commit the relay secret or OAuth credential files.
