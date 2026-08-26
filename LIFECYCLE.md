# Repository lifecycle

**State:** `active-bounded`  
**Decision date:** 2026-08-26  
**Review cadence:** quarterly  
**Next portfolio review:** November 2026

## Why this state

The repository is now an active, bounded producer of immutable `research.census-sample/v1` releases. PR #3 established deterministic household selection, stable namespaced person/household IDs, manifests, QA, checksums and a concrete downstream consumer in `indice-pobreza-UBA`.

Active does not mean open-ended feature development. Work should remain tied to named consumers and explicit contracts.

## Active boundary

The repository owns deterministic sample selection and stable sample identity, person↔household membership in the sampled release, sampling probability, approved sample weights, QA, manifests, checksums, packaging and detached verification.

It does not own Census source/geography authority, poverty methodology, EPH preprocessing, model training, model selection or income-model scientific decisions.

## Development policy

- Preserve deterministic, order-independent household selection and fail-closed validation.
- Expand schemas or execution surfaces only for a named consumer and explicit contract.
- Keep raw Census microdata and generated real releases outside Git.
- Treat privacy, lawful access and redistribution rights as operator responsibilities.
- Keep the historical sampling CLI compatible where practical, but do not let it define the current product boundary.

## Verification boundary

A meaningful real-release verification should record the exact local Census source, geography binding, command/parameters, deterministic seed behavior, row counts, output hashes, QA, and data-rights/privacy handling.

Fixture verification remains available through the repository Make targets and release checker.
