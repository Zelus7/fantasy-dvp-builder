# Cloudflare setup

## Existing production app: routine releases

Do **not** repeat the bootstrap steps below for the existing app. Its access code,
session-signing secret, credential-encryption key, and ingest token already live
in Cloudflare. Rotating them just to satisfy CI can invalidate logins or make saved
ESPN credentials unreadable. Normal Wrangler deployments preserve these secrets.

The manually dispatched **Deploy private web app** workflow needs only
`CLOUDFLARE_API_TOKEN` in GitHub's `production` environment. The account ID and public
app URL are non-secret workflow configuration. The token should be account-owned
where supported, limited to deploying the `fantasy-command-center` Worker and
applying migrations to its D1 database. Use resource-level permissions where
available; never grant account administration or unrelated Worker access just to
make CI pass. Obtain owner approval for the exact permissions before creating or
storing the token. Do not reuse a local OAuth token or the account's global API key.

1. Under repository **Settings → Environments → production**, save the approved
   token as `CLOUDFLARE_API_TOKEN`. Never paste it into chat, logs, issues, or source.
2. Dispatch the workflow for the reviewed release ref. It remains manual; no
   automatic deployment on push has been added. The environment name alone is
   **not** a reviewer gate: configure required reviewers separately if desired and
   supported by the repository's GitHub plan.
3. The workflow runs CI without deployment credentials, then reads secret **names**
   from Cloudflare and compares them with `wrangler.jsonc` before any migration.
   It fails closed on missing secrets or unreadable metadata and never sets secrets.
4. Apply migrations, deploy code with the existing secrets, and check `/api/health`,
   `/api/readiness`, and the private API's unauthenticated `401 AUTH_REQUIRED` response.
5. In the existing signed-in app, verify login continuity and current roster loading.
   The unauthenticated smoke check does not prove ESPN sync or authenticated login.

If smoke checks fail, inspect the run and live app before choosing a rollback;
database migrations are not automatically undone. Missing runtime secrets require
an explicit bootstrap/recovery decision, not blind regeneration. The dataset
refresh workflow still uses its separate existing `DATA_INGEST_TOKEN`.

References: [GitHub deployment authentication](https://developers.cloudflare.com/workers/ci-cd/external-cicd/github-actions/),
[permission scoping](https://developers.cloudflare.com/workers/authorization/), and
[Wrangler secret preservation](https://developers.cloudflare.com/workers/wrangler/commands/workers/).

## New installation only: bootstrap

## 1. Create D1

```bash
npm install
npx wrangler login
npx wrangler d1 create fantasy-command-center
```

Copy the returned database ID into `wrangler.jsonc`. A D1 database ID identifies the binding target but does not authorize access; API tokens and runtime secrets remain outside the repository.

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

## 4. Configure GitHub

Deployment: put `CLOUDFLARE_API_TOKEN` in the `production` environment, as above.
The account and D1 IDs identify resources but do not grant access; update their
non-secret workflow / Wrangler configuration for a different installation.

Dataset refresh: repository **Settings → Secrets and variables → Actions**:

- `APP_BASE_URL`
- `DATA_INGEST_TOKEN`

Use the same ingest token already set on the Worker. Do not duplicate the app access
code, session secret, or credential-encryption key into GitHub for routine releases.

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
