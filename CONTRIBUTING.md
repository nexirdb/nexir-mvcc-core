# Contributing

Thank you for considering a contribution to `nexir-mvcc-core`.

This crate is intentionally small and deterministic. Contributions should preserve the core boundaries:

- no wall-clock access in `src/`,
- no Tokio, thread spawning, or background tasks in `src/`,
- no durable-storage, consensus, database-runtime, or command-protocol dependencies in the core crate,
- no hot-path atomic metrics or global metrics registry in `src/`,
- no blocking wait queues or deadlock detector in the core.

## Development Checks

Run these before submitting changes:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo doc --no-deps --all-features
cargo package
```

For performance-sensitive changes, also run:

```bash
cargo bench --bench perf_truth -- --sample-size 10 --warm-up-time 0.1 --measurement-time 0.1
cargo run --example command_load_sim --release
```

## Design Expectations

- Prefer deterministic state transitions over background coordination.
- Keep adapter concerns in adapters: consensus, durability, command parsing, idempotency caches, metrics aggregation, and runtime scheduling.
- Add tests for every behavioral change.
- Update docs when public semantics, error behavior, or adapter contracts change.

## Licensing and DCO

`nexir-mvcc-core` is dual-licensed under either the MIT License or the
Apache License, Version 2.0, at the user's option. Contributions are accepted
under the same dual-license terms (inbound = outbound).

This crate uses the Developer Certificate of Origin (DCO), not a Contributor
License Agreement. Every commit must include a `Signed-off-by:` trailer using
your real name or an established identity (a known pseudonym you consistently
maintain) and a reachable email:

```bash
git commit -s -m "Your commit message"
```

Commits without a valid sign-off cannot be merged.
