# CODING_STANDARDS.md

## PILLAR 1: PYTHONIC EXCELLENCE
- **TYPE HYGIENE**: Prefer type hints throughout the codebase. Require them for core modules, public interfaces, and non-trivial functions.
- **IDIOMATIC CODE**: Use comprehensions, generators, and context managers where they improve clarity.
- **STANDARD LIBRARY FIRST**: Prefer stdlib solutions before adding dependencies.
- **DOCSTRINGS**: Use Google- or NumPy-style docstrings for public modules, classes, and functions.
- **READABILITY**: Write code for the next maintainer. Clarity beats cleverness.

## SIMPLICITY FIRST
- Prefer simple functions over complex abstractions.
- Avoid unnecessary classes and indirection.
- Keep functions focused and reasonably small.
- Write code that is easy to test, refactor, or delete.
- Do not optimize for hypothetical future requirements.

## ASYNC & SYNC BOUNDARIES
This repo may contain both styles. Keep boundaries explicit.

- In async paths, avoid blocking calls.
- Offload blocking I/O or CPU-heavy work via `asyncio.to_thread` or an executor when appropriate.
- In sync paths, do not introduce async unless required.
- Provide separate async and sync entry points or thin wrappers where necessary.
- Do not mix async and sync behavior within a single function or call stack without a clear boundary.

## ASYNC/SYNC INTEROP RULES
- No blocking calls inside `async def`.
- Do not call `asyncio.run()` from library code or inside an existing event loop.
- Use `asyncio.run()` only at top-level entry points such as CLI scripts when appropriate.
- Let the application or framework manage the event loop.
- If exposing both interfaces, prefer one primary implementation with thin wrappers, or maintain clearly separated parallel implementations.
- Do not share connection or client instances across async and sync code paths.

## ERROR HANDLING
- Fail clearly and early when inputs or assumptions are invalid.
- Raise specific exceptions where that improves diagnosis.
- Do not swallow exceptions silently.
- Add context to errors that cross I/O, DB, network, filesystem, or LLM boundaries.
- Prefer explicit return contracts over ambiguous sentinel values.

## LOGGING
- Use structured, purposeful logging.
- Log at important boundaries: external API calls, DB operations, filesystem operations, long-running jobs, and LLM interactions.
- Avoid excessive logging inside pure logic functions.
- Never log secrets, tokens, or sensitive personal data.
- Make logs useful for debugging, not noisy by default.

## TESTABILITY
- Write code that can be tested without heavy environment setup.
- Isolate side effects behind functions or adapters.
- Prefer deterministic behavior in tests.
- Mock external systems and non-deterministic dependencies.
- Cover edge cases when logic is non-trivial.

## DATA & CONFIG SAFETY
- Never hardcode secrets.
- Read secrets from `.env` or the established configuration system.
- Validate configuration at startup or at the first safe boundary.
- Treat filesystem, network, database, and model inputs as untrusted.
- Parameterize SQL and avoid unsafe string construction.

## STYLE CONSISTENCY
- Follow repository formatters and linters when configured.
- Otherwise, match the style of nearby code.
- Use descriptive names.
- Keep comments meaningful and avoid narrating obvious code.
- Prefer explicit imports and straightforward control flow.
