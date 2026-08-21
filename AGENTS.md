# Repository Agent Guidance

## Mobile-safe Codex tool use

Tools are expected and should be used whenever they improve correctness or speed. Keep the conversation payload bounded so Codex Remote can hydrate reliably on mobile:

- Keep displayed output from each command under roughly 20 KB. Save verbose logs, generated reports, and large JSON under `.artifacts/`, then report the exit status, key counts, and a short tail or targeted excerpt.
- Before printing an unfamiliar file or result, check its size. Use `rg`, focused `jq` projections, `head`, or `tail`; never dump generated bundles, minified assets, databases, dependency trees, or large type declarations into the thread.
- Preserve real failures. If a build or test fails, return its nonzero status with the smallest useful diagnostic excerpt and the saved log path.
- Keep generated Cloudflare types out of the conversation diff. `worker-configuration.d.ts` is generated and ignored; regenerate it locally when tooling requires it, but do not patch, print, or commit the full file.
- Prefer several focused commands over one command that emits a very large mixed diagnostic payload.
- For long-running commands, use an execution path that returns the terminal result to the original tool call. Do not abandon or orphan an active command at turn completion.

These limits apply to displayed transcript payload, not to Codex's ability to use tools or inspect files locally.
