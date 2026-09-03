---
applyTo: "main.py,api/**/*.py,frontend/**"
---

# Operator API and dashboard

- Keep FastAPI handlers thin and move reusable job, configuration, storage, or
  domain behavior to the owning module.
- Preserve each established authentication boundary. Operator job and
  configuration mutations use the configured Admin role, while ingestion
  endpoints retain their API-key contract. Do not weaken authentication or
  broaden read exposure without an explicit security decision.
- Validate external input and return actionable HTTP errors without leaking
  secrets, document content, or internal credentials.
- Preserve API response compatibility or coordinate frontend and
  documentation updates in the same change.
- Keep frontend types aligned with API payloads and reuse existing components,
  tokens, and interaction patterns.
- Use the scripts in `frontend/package.json`: `npm test`, `npm run lint`, and
  `npm run build`.
- Test loading, empty, success, failure, authorization, polling/timer cleanup,
  and accessibility-relevant behavior as applicable.
