# Security model

## ESPN credentials

The app never collects or stores an ESPN password. A private Manifest V3 extension reads only `SWID` and `espn_s2` from the user's already-authenticated ESPN browser session.

Cookies are:

- Sent only over HTTPS to the configured private app URL.
- Encrypted before D1 storage with AES-256-GCM and a Worker secret.
- Never returned through an API response after sync.
- Redacted from application errors.
- Automatically marked expired or degraded when ESPN rejects them.

## Extension authorization

The app generates a single-use, eight-character code valid for ten minutes. Codes are stored only as SHA-256 hashes. Successful pairing issues a high-entropy device token; only its hash is stored in D1. Devices are individually revocable from Settings.

## App authentication

Private v1 uses a server-side access code to mint a signed HttpOnly, Secure, SameSite=Strict session cookie. The access code is a Cloudflare secret, not a client-side value.

## Data pipeline

Pipeline ingestion uses a separate bearer secret and is not accessible with an app session or extension token. Payloads are validated and staged before activation.

## Browser and transport controls

- Strict Content Security Policy.
- No third-party scripts.
- Frame embedding denied.
- Camera, microphone, geolocation, and payment permissions disabled.
- Extension CORS limited to extension origins.
- Login, pairing, and sync rate limiting.

## Logging

Do not log request bodies, Cookie headers, Authorization headers, encryption material, access codes, or ESPN responses containing user details. Telemetry should be limited to operation, status, duration, league hash, source freshness, and error class.

## Incident response

1. Revoke the affected browser device in Settings.
2. Rotate `APP_ACCESS_CODE` if app access is in doubt.
3. Rotate `DATA_INGEST_TOKEN` if the pipeline token is in doubt.
4. Rotate `SESSION_SECRET` to invalidate all app sessions.
5. Rotate `CREDENTIAL_ENCRYPTION_KEY` only with a credential re-sync plan; existing encrypted ESPN cookies become unreadable.
6. Review Cloudflare and GitHub audit logs.
