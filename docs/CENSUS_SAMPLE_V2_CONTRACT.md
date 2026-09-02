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
