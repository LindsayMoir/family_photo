## Test Scaffold

This repository now includes the `pytest` scaffold expected by `TEST_PLAN.md`:

- `tests/unit`
- `tests/integration`
- `tests/e2e`
- `tests/fixtures/images`
- `tests/fixtures/embeddings`
- shared fixtures in `tests/conftest.py`

## Current Scope

The current codebase does not yet implement dedicated `embedding` or `clustering` modules. The initial automated coverage therefore targets the deterministic pipeline stages and orchestration that exist today:

- configuration loading
- crop filesystem behavior
- detection review/path helpers
- orientation helper behavior
- pipeline orchestration smoke paths

## When Face Identity Stages Arrive

When `embedding` and `clustering` code is added, extend the existing scaffold with:

- deterministic embedding fixtures in `tests/fixtures/embeddings`
- unit tests for vector dimensionality and similarity behavior
- integration tests for embedding-to-clustering handoff
- end-to-end tests using curated image fixtures in `tests/fixtures/images`
