# `research.census-target-year-sample/v2`

Status: **implemented contract; real CPV-2022 local proof pending**

v2 is the vintage-neutral successor to the current CPV-2010-coded target-year release.

## Scientific definition

Given donor person mass `D[d]`, target person mass `T[d,y]`, and global sampling intensity `c`:

```text
p[d,y] = c * T[d,y] / D[d]
```

Households are selected deterministically with the current SHA-256 household/department score and all members of each selected household are retained.

The deterministic score deliberately preserves the pre-retrofit CPV-2010 byte contract:

```text
score(seed, frame_household_id, department_id)
```

Target year is absent so one frame uses common random numbers across 2024/2025. Frame identity does not perturb the migration score; instead it namespaces sample/release identities so different Census frames cannot collide.

## Authoritative sampling artifact

`selection.parquet` defines the selected household sample independently from substantive Census columns.

Core fields:

```text
sample_household_id
frame_household_id
frame_dwelling_id
department_id
radio_id
household_person_count
selection_probability
design_inverse_probability_weight
selection_score
```

## Complete person membership

`person_membership.parquet` contains:

```text
sample_person_id
frame_person_id
sample_household_id
frame_household_id
```

Its membership count per household must exactly equal the donor frame's `household_person_count`.

## Materialization modes

### `selection-only`

Produces:

```text
manifest.json
qa.json
selection.parquet
person_membership.parquet
```

No substantive Census payload is copied.

### `full-payload`

Additionally materializes the selected relational Census records:

```text
vivienda.parquet
hogar.parquet
persona.parquet
```

using explicit key filters against the already fixed selection. All source columns are preserved.

## Identity

v2 sample IDs are frame-aware, conceptually:

```text
census:<frame-namespace>:household:<frame-household-id>
census:<frame-namespace>:person:<frame-person-id>
```

They are deterministic and independent of sampling target year for the same frame.

## Weight semantics

The contract keeps separate:

```text
selection_probability
design_inverse_probability_weight = 1 / selection_probability
analysis_weight = unset
generic_sample_weight = unset
```

The inverse-probability quantity is a design/audit field; it is not automatically authorized as a model or target-year analysis weight.

## Validation

`validate_sample_release_v2()` verifies at least:

- artifact hashes;
- unique household/person sample identity;
- person-to-selected-household membership;
- complete household membership counts;
- selection probability domain;
- exact inverse-probability relation;
- QA counts;
- absence of invented generic analysis weights.

The builder additionally checks full-payload row counts against the fixed key selection.

## Migration guarantee

CI compares the existing CPV-2010 streaming sampler directly with the new frame-based sampler for both 2024 and 2025 and requires the same selected source households, persons and household probabilities.

The file schema and sample IDs are intentionally different because v2 is a new contract; the scientific selection is required to remain identical.


## Hydrating an existing selection-only release

A governed `selection-only` release may be promoted to a new `full-payload`
child without rerunning or reinterpreting the sampling design:

```bash
python -m censo_sampler.frontdoor materialize-selection \
  --frame /path/to/exact/research.census-frame-v1-release \
  --selection-release /path/to/existing-selection-only-release \
  --output-root /path/to/output-root
```

This operation is deliberately stricter than an ad-hoc key filter.

Before materialization it verifies the complete frame artifact hashes and requires
the frame release ID and frame-manifest SHA-256 to match the exact parent recorded
by the selection-only release. It then reuses `selection.parquet` and
`person_membership.parquet` byte-for-byte; no household score is recomputed and
no resampling is permitted.

The selected `vivienda`, `hogar` and `persona` tables are filtered from the
parent frame payload with all columns intact. The child manifest records complete
Arrow schemas and deterministic schema fingerprints for both the frame payload
and the materialized child. Any parent/output schema disagreement fails closed.

The existing selection-only release remains immutable. The operation emits a new
content-addressed full-payload child carrying explicit lineage back to both the
selection release and exact frame manifest.

This is also the supported diagnostic path when a local copy appears to have lost
substantive columns: do not invent semantic recodes to compensate for a custody
or schema mismatch. First prove exact frame bytes and schema preservation.
