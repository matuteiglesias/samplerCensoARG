# Repository lifecycle

**State:** `active-bounded`  
**Decision date:** 2026-08-26  
**Review cadence:** active-development

## Why this state

The repository is an active, bounded producer of immutable `research.census-sample/v1` releases. PR #3 established deterministic household selection, stable namespaced person/household IDs, manifests, QA, checksums and concrete downstream consumers.

Active does not mean open-ended feature development. Work should remain tied to named consumers and explicit contracts.

## Active boundary

The repository owns deterministic **household** sample selection and stable sample identity, person↔household membership in the sampled release, sampling probabilities, declared sampling-design semantics, QA, manifests, checksums, packaging and detached verification.

It also owns the optional target-year sampling design in which an exact population-by-department source changes department-level household selection probabilities so that a sufficiently large synthetic sample approximates the target year's department composition.

This is part of sampling, not a post-sampling population-calibration layer. The sampled units remain Census-2010 donor records.

It does not own Census source/geography authority, demographic projection methodology, welfare inference, poverty methodology, EPH preprocessing, model training, model selection or income-model scientific decisions.

## Target-year sampling interpretation

For target year `y`, the historical durable idea is:

```text
relative_department_size[d, y] = population[d, y] / population[d, 2010]
selection_probability[d, y] = base_fraction * relative_department_size[d, y]
```

with explicit probability bounds and an exact pinned population source.

The design assumes that sufficiently large random household samples from the 2010 donor frame remain statistically useful for dimensions that are not explicitly updated within departments. Only department mass is deliberately changed by the target-year population information.

See `docs/TARGET_YEAR_HOUSEHOLD_SAMPLING.md` for the full interpretation and limitations.

## Current implementation gate

The governed release implementation is **not yet approved for real target-year sampling**.

The current `legacy_department_projection_candidate` path and generic `sample_weight` field retain historical/overloaded semantics. A modern implementation must first separate:

- `selection_probability`;
- optional `design_inverse_probability_weight` for inference back to the donor frame;
- any downstream `analysis_weight` authorized for the target-year synthetic-sample estimand.

Applying inverse-probability weights mechanically after deliberately changing selection probabilities by department can undo the target-year composition. No real target-year poverty run should proceed until that ambiguity is removed.

## Development policy

- Preserve deterministic, order-independent household selection and fail-closed validation.
- Keep household as the primary sample unit for the current use case; all members of selected households remain linked.
- Require exact, versioned population-by-department evidence before enabling target-year sampling in the governed release path.
- Keep frame vintage and sampling target period distinct.
- Expand schemas or execution surfaces only for a named consumer and explicit contract.
- Keep raw Census microdata and generated real releases outside Git.
- Treat privacy, lawful access and redistribution rights as operator responsibilities.
- Keep the historical sampling CLI compatible where practical, but do not let it define the current product boundary.

## Verification boundary

A meaningful real-release verification should record the exact local Census source, geography binding, sampling target period, population-by-department parent when used, command/parameters, deterministic seed behavior, row counts, selection probabilities, authorized analysis-weight semantics, output hashes, QA and data-rights/privacy handling.

Fixture verification remains available through the repository Make targets and release checker.
