# Cloudflare setup

## 1. Create D1

```bash
npm install
npx wrangler login
npx wrangler d1 create fantasy-command-center
```

Copy the returned database ID into `wrangler.jsonc` for local work. GitHub deployment injects `CLOUDFLARE_D1_DATABASE_ID` at runtime, so the production ID is not committed.

Apply the schema:

```bash
npx wrangler d1 migrations apply fantasy-command-center --remote
```

## 2. Set Worker secrets

```bash
npx wrangler secret put APP_ACCESS_CODE
npx wrangler secret put SESSION_SECRET
npx wrangler secret put CREDENTIAL_ENCRYPTION_KEY
npx wrangler secret put DATA_INGEST_TOKEN
```

- `SESSION_SECRET`: at least 32 random characters.
- `CREDENTIAL_ENCRYPTION_KEY`: exactly 32 random bytes encoded as base64.
- `DATA_INGEST_TOKEN`: a separate high-entropy value shared only with the refresh workflow.

## 3. Deploy a test domain

```bash
npm run deploy
```

Cloudflare returns a `workers.dev` URL. Use that for private acceptance before adding a custom domain.

## 4. Configure GitHub secrets

Repository **Settings → Secrets and variables → Actions**:

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_D1_DATABASE_ID`
- `APP_BASE_URL`
- `DATA_INGEST_TOKEN`

Use a narrowly scoped Cloudflare API token. Do not paste any of these values into chat or an issue.

## 5. Connect ESPN

1. Sign into the deployed app.
2. Generate a pairing code in Settings.
3. Load the `extension/` directory unpacked in Chrome or Edge.
4. Sign into ESPN normally.
5. Enter the app URL and pairing code in the extension.
6. Confirm the league list and default league in the app.

## 6. Build initial datasets

Run **Refresh NFL intelligence data** from GitHub Actions. During preseason, pass `through_week=1`; prior-year blending supplies the initial baseline until current games accumulate.

## Custom domain

After acceptance, add a Worker custom domain in Cloudflare, then update the extension `host_permissions` to include it before rebuilding the unpacked extension. The `workers.dev` URL remains a rollback path.
