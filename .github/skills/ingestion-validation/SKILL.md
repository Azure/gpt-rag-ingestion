---
name: ingestion-validation
description: Selects and runs evidence-based validation for Python ingestion, chunking, jobs, Azure AI Search contracts, deployment assets, and the operator frontend. Use whenever repository behavior or engineering assets change.
---

# Ingestion validation

Start with the narrowest existing command that covers the change.

## Engineering-agent assets

```text
python -m pip install --requirement .github/scripts/requirements.txt
python .github/scripts/validate-agentic-assets.py
```

## Python

Run focused tests first:

```text
python -m pytest tests/<affected_test>.py
```

Then run the maintained suite when risk or repository policy requires it:

```text
python -m pytest
```

Do not require live Azure credentials for unit tests. Reuse existing fakes,
stubs, and golden fixtures.

## Frontend

From `frontend/`, use the scripts defined in `package.json`:

```text
npm test
npm run lint
npm run build
```

## Runtime integration

For changes to formats, sources, jobs, embedding, Search payloads, or
deployment:

1. Build and run the existing container or use the applicable
   `scripts/deploy.ps1` or `scripts/deploy.sh` path.
2. Exercise a representative source and document.
3. Verify the terminal job result and expected Azure AI Search documents,
   fields, ACL metadata, and deletion behavior.
4. Inspect logs and correlated audit events without copying document content,
   credentials, or private environment names into public artifacts.

If a command is unavailable or live validation is unsafe, report the missing
dependency and residual risk. Do not substitute an unrelated passing check.
