# Python implementation

Python 3.12 is the repository runtime. Existing code and configured checks
take precedence over generic style preferences.

- Keep modules cohesive and names intent-revealing.
- Prefer explicit type hints and dataclasses or validated models at
  boundaries.
- Preserve async end to end. Use async Azure clients in coroutines, close
  clients and credentials, and bound concurrency.
- Avoid broad exception handling. Catch expected SDK or parsing failures at
  the boundary where recovery or actionable context is available.
- Never hide an indexing, deletion, configuration, or authorization failure
  behind an empty result or success-shaped fallback.
- Use the configured logger; do not add `print`.
- Keep pure transformations independent from Azure SDK imports when practical
  so they can be tested without credentials.
- Do not introduce a new formatter, linter, type checker, or test framework
  when the repository does not already use it.

For public modules or extension points, document behavior, inputs, outputs,
side effects, exceptions, and constraints rather than restating signatures.
