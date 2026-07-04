# Security Policy

`nexir-mvcc-core` is a standalone Rust library. It does not expose a network service, parse external protocols, spawn background tasks, or perform disk I/O by itself.

Security-sensitive behavior usually appears in adapters that embed the core, especially around:

- durable backend crash safety,
- replay/idempotency records,
- snapshot/restore correctness,
- command parsing and authorization,
- distributed consensus integration.

## Reporting Issues

Please report suspected security issues privately before public disclosure. Use GitHub private vulnerability reporting for this repository. If that is unavailable, contact a Nexir maintainer through a private channel and include:

- affected version or commit,
- a minimal reproduction if possible,
- expected vs. actual behavior,
- whether the issue affects only an adapter or the core crate itself.

## Supported Versions

Until the project reaches a stable 1.0 release, security fixes target the latest published 0.x release line and the main development branch.
