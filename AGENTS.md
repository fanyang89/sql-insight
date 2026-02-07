# Repository Guidelines

## Project Structure & Module Organization
This repository is a Rust project centered on MySQL observability logic.

- `Cargo.toml`: package metadata and dependencies.
- `src/main.rs`: executable entry point (local demo/output).
- `src/lib.rs`: library exports.
- `src/collection.rs`: core collection-level negotiation logic and unit tests.

Keep new domain logic in focused modules under `src/` (for example, `src/probe.rs`, `src/analyzer.rs`). Add integration tests in `tests/` when cross-module behavior grows.

## Build, Test, and Development Commands
Use standard Cargo workflows:

- `cargo check`: fast type/compile validation.
- `cargo run`: run the CLI/demo binary locally.
- `cargo test`: run all unit and integration tests.
- `cargo fmt`: apply Rust formatting.
- `cargo clippy --all-targets --all-features`: lint common issues before PR.

Run `cargo fmt && cargo test` before opening a pull request.

## Coding Style & Naming Conventions
Follow idiomatic Rust (edition 2024) with `rustfmt` defaults (4-space indentation, no tabs).

- Types/enums/traits: `PascalCase` (e.g., `CollectionPolicy`).
- Functions/modules/variables: `snake_case` (e.g., `negotiate_collection_level`).
- Constants: `SCREAMING_SNAKE_CASE`.

Prefer small, single-purpose functions and explicit structs for policy/capability inputs. Keep comments brief and only where intent is not obvious from code.

## Testing Guidelines
Use Rust’s built-in test framework with `#[test]` and `#[cfg(test)]`.

- Co-locate unit tests with the module they validate.
- Name tests by behavior, e.g., `should_downgrade_to_level1_when_performance_schema_unavailable`.
- Cover both success and downgrade/fallback paths for collection-level negotiation.

Execute tests with `cargo test`.

## Commit & Pull Request Guidelines
Current history uses short imperative commit subjects (example: `Create project`). Continue with concise, action-first messages:

- `Add collection level evaluator`
- `Refine level2 sys schema gating`

PRs should include:

- What changed and why.
- Risk/rollback notes for behavior changes.
- Verification steps and command results (at minimum `cargo test`).

## Security & Configuration Tips
Do not commit credentials, production SQL text with sensitive literals, or host-specific log dumps. Treat Level 3 diagnostics (e.g., packet/system-call sampling) as explicit, short-window, opt-in operations.
