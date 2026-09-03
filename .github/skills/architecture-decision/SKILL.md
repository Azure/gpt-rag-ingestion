---
name: architecture-decision
description: Conducts and records a verifiable ingestion architectural decision. Use when a choice alters pipeline boundaries, chunk/index contracts, identity, data, source integration, deployment, or operation with meaningful reversal cost.
---

# Ingestion architectural decision

1. Load the relevant `engineering-principles` references.
2. Define the retrieval or operator outcome, constraints, affected
   repositories, and up to five prioritized characteristics with measures.
3. Compare at least two viable alternatives and not changing.
4. Evaluate document security, data contracts, index compatibility, chunk
   quality, throughput, memory, cost, failure recovery, migration, and
   reversibility.
5. Record the decision under `docs/adr/` using
   [the ADR template](references/adr-template.md).
6. Define fitness functions, adoption order, rollback or reindex strategy,
   and a review trigger.

Do not turn a tool or Azure service preference into an architectural
requirement. When evidence is missing, record a time-bounded investigation
and its decision criterion rather than guessing.
