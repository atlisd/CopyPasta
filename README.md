# copypasta

A tiny self-hosted clipboard for moving text (or an image) between computers,
live. Open a channel on one machine, join it with the 6-digit PIN on the other,
and everything you send lands on the far screen immediately — no refresh, no
re-entering the PIN. The channel self-destructs when its timer runs out.

- Go binary with an embedded single-page UI — no npm, no build step, ~15 MB image
- **WebSocket push**: the receiver updates the instant the sender presses Send
- The PIN stays open as a channel: press Send again to update the far screen
- Redis with persistence switched off (`--save "" --appendonly no`), so pastes
  live in memory only and expiry is genuinely the end of them
- PIN gates reading; a separate write token (held only by the sender) gates
  writing, so a reader can never overwrite the channel
- 5 wrong PINs burn the channel, and joins are rate-limited
- Text is the main event; pasted or dropped images work too

## How it works

1. **Send** — type or paste, press **Send**. You get a PIN, and the button
   becomes **Update**. `⌘/Ctrl + Enter` sends too.
2. **Receive** — type the PIN into the other machine. It connects (the dot next
   to *Receive* goes green: `LIVE`) and shows whatever is on the channel.
3. Press **Send** again on the source at any time — the far screen updates
   instantly and, with *Copy to clipboard automatically* ticked, the value is
   already on your clipboard.

Each Send slides the expiry out by a full TTL, so a channel in active use stays
alive while an abandoned one still dies on schedule. The receiver reconnects on
its own with backoff if the network hiccups or the tab is backgrounded.

## Run it locally

```sh
docker compose up --build
# http://localhost:8080
```

## Run it on the Docker host

Copy `docker-compose.prod.yml` and `.env.example` to the server, fill in the env
file, then:

```sh
cp .env.example .env   # set GHCR_OWNER and CLOUDFLARE_TUNNEL_TOKEN
docker compose -f docker-compose.prod.yml up -d
```

The app publishes no host port. In the Cloudflare Zero Trust dashboard, give the
tunnel a public hostname pointing at **`http://app:8080`** — `cloudflared` shares
the compose network with the app and reaches it by service name. Cloudflare
Tunnel proxies WebSockets by default, so the live updates work through it with
no extra configuration.

To pick up a new release:

```sh
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d
```

## Releasing

Push a version tag — that is the only thing that triggers CI. It runs the tests,
builds a multi-arch image (amd64 + arm64), pushes it to
`ghcr.io/<owner>/copypasta`, and cuts a GitHub Release for the tag:

```sh
git tag v0.9.0
git push origin v0.9.0
```

Image tags produced: `v0.9.0` → `0.9.0`, `0.9`, and `latest`. The Release carries
the image reference, its digest, and the deploy commands, followed by
auto-generated notes from the commits since the last tag.

If the package is private, run `docker login ghcr.io` on the server once with a
PAT that has `read:packages`. The first push of a package is private by default —
make it public (or grant the server's PAT access) under the package settings on
GitHub.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | Listen port |
| `REDIS_ADDR` | `redis:6379` | Redis host:port |
| `REDIS_PASSWORD` | *(empty)* | Set if your Redis needs auth |
| `MAX_PASTE_BYTES` | `5242880` | Largest accepted request body |
| `DEFAULT_TTL_SECONDS` | `600` | TTL when the client doesn't pick one |

The UI offers 1 minute / 10 minutes / 1 hour; the server rejects anything else.

## API

| Method | Path | Body | Returns |
|---|---|---|---|
| `POST` | `/api/channel` | `{ttl}` | `{pin, token, expires_in}` |
| `POST` | `/api/publish` | `{pin, token, kind, mime, data, ttl}` | `{expires_in}` |
| `POST` | `/api/fetch` | `{pin}` | `{kind, mime, data, expires_in, empty}` |
| `GET` | `/api/subscribe?pin=` | — | WebSocket stream of the same update shape |
| `GET` | `/healthz` | — | `{"status":"ok"}` |

`kind` is `"text"` or `"image"`; image `data` is base64. `ttl` is seconds. The
socket sends a snapshot of the current value on connect, then every update after
it, so a receiver that joins late is never left staring at an empty box.

## A note on the threat model

The PIN keeps a channel from being readable to anyone who merely loads the page,
and the tunnel keeps the site off the open internet. It is not end-to-end
encryption: the server sees your data while it is stored. Don't put anything in
here you wouldn't put in a chat message — and for real secrets, use a shorter TTL.

## Development

```sh
go test ./...
go run .          # needs a Redis at REDIS_ADDR
```
