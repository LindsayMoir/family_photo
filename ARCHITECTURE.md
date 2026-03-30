# ARCHITECTURE.md

## PILLAR 1: ARCHITECTURAL FOUNDATIONS
- **REUSE FIRST**: Scan the repository and current context for existing methods, utilities, and patterns. If something already exists, reuse or adapt it instead of duplicating logic.
- **SOLID & DRY**: Preserve single responsibility and avoid duplication, but do not introduce abstraction for its own sake.
- **COMPOSITION OVER INHERITANCE**: Favor shallow, modular designs.
- **KISS/YAGNI**: Keep solutions small and direct. Do not build what is not currently required.

## MVP ITERATION
This project is expected to evolve.

- Start with simple implementations.
- Prefer concrete code over abstract frameworks.
- Refactor when repeated patterns emerge.
- Accept temporary simplifications when they accelerate learning.
- Avoid overengineering the first version.

## PILLAR 2: RELIABILITY & DEFENSIVE DESIGN
- **FAIL FAST**: Validate inputs and assumptions at entry points.
- **ERROR HANDLING**: No silent failures. Use repository logging and exception patterns if present; otherwise use clear exceptions and structured logging.
- **TESTS**: Add or update tests near the changed code when practical. Mock external I/O and keep tests deterministic.
- **EDGE CASES**: Explicitly handle timeouts, empty states, boundary values, malformed data, and retries where appropriate.
- **IDEMPOTENCY**: Make operations safe to retry when feasible.

## PILLAR 3: PERFORMANCE & SECURITY
- **COMPLEXITY**: Avoid unnecessary O(n²) behavior where simpler O(n) or indexed strategies exist. Measure before optimizing hot paths.
- **SANITIZATION**: Treat all external data (user, API, filesystem, DB, LLM) as untrusted. Validate and sanitize inputs.
- **SECRETS & CONFIG**: Never hardcode secrets. Use environment variables or the existing config system. Parameterize SQL and avoid string interpolation.
- **RESOURCE SAFETY**: Close DB connections, files, and sockets explicitly; prefer context managers.
- **LEAST PRIVILEGE**: Minimize permissions and data exposure at every boundary.

## PILLAR 4: RESILIENCE
- **IMMUTABILITY**: Use constants and immutable structures where they improve safety and clarity.
- **DECOUPLING**: Keep modules loosely coupled. Avoid deep knowledge of internal details across layers.
- **RESILIENCE**: For external dependencies, use timeouts, retries with backoff, and graceful degradation where appropriate.
- **SIDE EFFECTS**: Prefer pure functions for core logic. Keep side effects explicit and isolated.

## LLM INTEGRATION
When integrating LLMs:
- Treat model outputs as untrusted input.
- Validate all structured outputs before use.
- Log prompts, model choices, and responses when useful for debugging, while protecting secrets and sensitive data.
- Prefer deterministic or constrained outputs for parsing workflows.
- Design retries and fallback behavior intentionally rather than implicitly.
- Never let raw model output directly drive destructive actions.

## DATA MODELING
- Prefer simple, explicit schemas.
- Avoid over-normalization in early phases unless it clearly helps.
- Optimize for readability, traceability, and future evolution.
- Keep naming consistent across models, APIs, and storage.
- Preserve raw source data when it may help debugging or reprocessing.

## DATABASE SAFETY
When modifying SQL or database logic:
- Assume production data may exist.
- Avoid destructive operations unless explicitly requested.
- Prefer migrations or reversible updates.
- Validate schema assumptions before writing queries.
- Always parameterize SQL queries.
- Make batch operations resumable where possible.

## SCRAPING / INGESTION RELIABILITY
When modifying scraping, import, or parsing logic:
- Assume upstream sources may change.
- Handle missing or malformed fields gracefully.
- Log failures clearly enough for diagnosis.
- Avoid brittle selectors and overly rigid parsing.
- Prefer resilient extraction strategies and preserve source context when useful.

## MINIMAL CHANGES
- Prefer small, targeted modifications.
- Do not rewrite entire modules unless explicitly requested.
- Preserve existing working logic unless there is a clear defect or simplification opportunity.
- Match the scale of the solution to the scale of the problem.
