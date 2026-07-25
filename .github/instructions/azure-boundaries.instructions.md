---
applyTo: "dependencies.py,tools/**/*.py,infra/**,scripts/**,azure.yaml,Dockerfile"
---

# Azure, sources, configuration, and deployment

- Read runtime settings through the existing configuration provider. Preserve
  the `gpt-rag-ingestion`, `gpt-rag`, and no-label selectors, and verify the
  provider's effective override behavior before changing their order.
- Resolve secrets through Key Vault references. Never hardcode endpoints,
  resource names, index names, container names, credentials, or flags.
- Prefer managed identity and least-privilege RBAC; preserve the established
  Azure CLI local-development fallback.
- Use the shared credential/client helpers and close async clients and
  credentials appropriately.
- Set explicit timeouts, bounded retries, and actionable errors at external
  boundaries.
- Keep `scripts/deploy.ps1` and `scripts/deploy.sh` behaviorally aligned.
- Do not log tokens, connection strings, document content, personal
  environment names, or resource-group names.
- Load `engineering-principles` for identity, network, source, Search, storage,
  or deployment changes and `documentation-consistency` for changed operator
  steps or configuration.
