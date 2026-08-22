# Security Policy

## Reporting a vulnerability

Please report security issues **privately** via GitHub's
[private vulnerability reporting](https://github.com/tiny-systems/storage-module/security/advisories/new)
(this repo's **Security** tab → **Report a vulnerability**). Do not open public
issues for security problems.

## Known residual advisories

`govulncheck ./...` (source-reachability lens) is the source of truth and runs
against every release. The advisories below have **no upstream fix** and are
reachable only through build/install tooling — never the deployed runtime
(`/manager run`) components:

- **github.com/containerd/containerd** — pulled transitively by the Helm client
  used only in the `pre-install` / `pre-delete` chart hooks. No patched release.
- **golang.org/x/crypto** — pulled by NATS `nkeys` (curve25519) for transport
  auth. No patched release.

Dependabot reports against the whole dependency graph, not reachability, so it
lists these until upstream ships fixes. The grouped Dependabot config in
`.github/dependabot.yml` opens a PR automatically when a fix lands.
