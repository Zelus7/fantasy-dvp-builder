# Security policy

This repository contains a private application but no production credentials. Report security issues privately to the repository owner rather than opening a public issue.

Never include ESPN `SWID`, `espn_s2`, Cloudflare tokens, D1 exports, app access codes, session secrets, encryption keys, or ingest tokens in issues, pull requests, logs, screenshots, or chat messages.

Supported private-v1 releases receive fixes on the active release branch. Credential exposure should be handled by revoking the affected browser, rotating the relevant secret, reviewing audit logs, and resyncing ESPN if the encryption key changes.
