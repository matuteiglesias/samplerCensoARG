# Repository lifecycle

**State:** `active-bounded`  
**Decision date:** 2026-08-26  
**Current architecture review:** 2026-09-02  
**Review cadence:** active-development

## Why this state

The repository is an active, bounded producer of deterministic Census household samples. Its modern boundary is now expressed through two explicit contracts:

```text
research.census-frame/v1
research.census-target-year-sample/v2
```

The historical CPV-2010/v1 implementation remains in place as a compatibility surface and regression oracle, but it no longer defines the target architecture.

Active does not mean open-ended feature development. Work remains tied to the sampler's narrow authority: donor-frame validation/preparation where explicitly owned, household selection, complete household membership, sampling probabilities, release identity, QA and reproducibility.

## Active boundary

The repository owns:

- deterministic **household** sample selection;
- stable sample identity;
- complete person↔household membership for selected households;
- target-year department-level selection probabilities;
- the distinction between selection probability, design inverse-probability weight and downstream analysis weight;
- validation of `research.census-frame/v1`;
- the compatibility CPV-2010 CSV→Parquet frame builder;
- key-first sample artifacts;
- optional full relational VIVIENDA/HOGAR/PERSONA materialization after selection;
- QA, manifests, hashes and detached validation of sample v2;
- preservation of the historical/v1 paths for reproducibility.

It does **not** own:

- RXDB/RedEngine extraction;
- CPV-2022 source-specific extraction or hierarchy recovery;
- official Census publication/geography authority;
- EPH↔Census feature selection or semantic alignment;
- demographic target-population methodology itself;
- welfare inference, poverty methodology, model training or model selection;
- Census microdata redistribution rights.

## Vintage-neutral donor frames

The sampler now consumes a prepared donor frame rather than assuming that its source is three CPV-2010 CSV files.

Conceptually:

```text
CPV-2010 CSV ──> 2010 frame builder ──┐
                                      ├─> research.census-frame/v1
CPV-2022 RXDB ─> upstream adapter ────┘
                                      │
                                      ▼
                               shared sampler
```

The donor frame has a narrow sampling/index plane and a complete relational payload plane. Substantive Census variables are not part of the sampling decision.

The CPV-2022 frame producer belongs in `argentina-censo2022-rxdb`; this repository only checks and consumes its output.

## Target-year sampling interpretation

For target year `y`, department `d`, exact donor-frame person mass `D[d]`, target person mass `T[d,y]` and global sampling intensity `c`:

```text
p[d,y] = c * T[d,y] / D[d]
```

Households are the selection unit and all persons belonging to a selected household are retained.

The deterministic score deliberately excludes target year so 2024 and 2025 use the same pseudo-random household ordering within one donor frame. The migration also preserves the historical CPV-2010 score bytes so the new architecture can be tested for exact scientific parity against the prior streaming implementation.

Only department-level person mass is intentionally updated by the target-population parent. Within-department demographic, socioeconomic and household distributions remain donor-frame assumptions.

## Weight semantics are repaired in v2

The previous lifecycle gate warned that the historical generic `sample_weight` field overloaded incompatible meanings. Sample v2 resolves that architectural ambiguity by explicitly carrying:

```text
selection_probability
 design_inverse_probability_weight = 1 / selection_probability
 analysis_weight = unset
 generic_sample_weight = unset
```

The inverse-probability value is a design/audit quantity. It is not automatically authorized as a downstream model or target-year analysis weight.

The historical/v1 path remains available, but new scientific work should use the frame/v2 path.

## Current implementation gate

### Fixture/software gate — GREEN

As of 2026-09-02:

- the CPV-2010 CSV→full-Parquet frame builder is implemented;
- deep frame validation is implemented with disk-backed relational checks;
- the vintage-neutral household selection kernel is implemented;
- selected household keys and complete person membership are first-class artifacts;
- `selection-only` and `full-payload` materialization are implemented;
- sample v2 and its offline validator are implemented;
- the existing governed target-population parent can be consumed through a neutral department adapter without modifying its bytes;
- CPV-2010 fixture tests require exact household/person/probability parity between the previous streaming sampler and the new frame-based sampler for 2024 and 2025;
- a synthetic CPV-2022-shaped donor frame uses the exact same sampling/release code and preserves source-only 2022 columns;
- an additional fixture proves that no sex, age, EDAD or EPH-facing variable is required by the modern sampling path;
- Python 3.10 and 3.12 CI is green for the implemented frame/v2 boundary.

### Real-data gate — PENDING LOCAL MATERIALIZATION

Remaining acceptance work primarily requires data/runtime that intentionally lives outside Git:

1. build and deep-check the complete authorized CPV-2010 frame;
2. produce at least one real CPV-2010 v2 sample and record scale/runtime characteristics;
3. obtain the bounded real CPV-2022 relational extract from the upstream RXDB stack;
4. build/check the first real CPV-2022 frame, beginning with RADIO `061471101`;
5. run the same sampler against that real 2022 frame;
6. enumerate any actual department-code exceptions before a national 2022 target-year run;
7. only then materialize national 2022 frames/samples.

These are tracked in `docs/HUMAN_OPERATOR_QUEUE.md`.

## Development policy

- Preserve deterministic, order-independent household selection and fail-closed validation.
- Keep household as the primary sample unit; all members of selected households remain linked.
- Define a sample by selected keys before materializing substantive payload.
- Do not reintroduce EPH/model feature projection into the sampler.
- Require exact, versioned target-population evidence before scientific target-year runs.
- Keep donor-frame vintage and sampling target period distinct.
- Treat department code identity across vintages as an explicit provisional policy and surface actual exceptions rather than silently remapping them.
- Keep raw Census microdata, prepared real frames and generated real sample payloads outside Git.
- Treat privacy, lawful access and redistribution rights as separate operator/governance responsibilities.
- Preserve historical sampling interfaces where practical, but do not let them define new contracts.

## Verification boundary

A meaningful real-release verification records:

- exact donor-frame release and manifest hash;
- Census vintage and source identity;
- target-population parent identity;
- department-alignment diagnostics;
- target year, fraction and deterministic seed;
- sampling algorithm and probability formula;
- selected household/person counts;
- complete-membership assertions;
- materialization mode;
- artifact hashes;
- QA;
- data-rights/privacy handling.

Fixture verification is available through:

```bash
make check
```

The exact cross-repository and human/local gates are documented rather than represented as fake CI success.
