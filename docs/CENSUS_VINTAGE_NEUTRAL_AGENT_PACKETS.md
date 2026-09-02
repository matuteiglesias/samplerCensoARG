# Autonomous agent work packets — Census vintage-neutral retrofit

Companion to `docs/CENSUS_VINTAGE_NEUTRAL_RETROFIT_PLAN.md`.

These packets are intentionally bounded so multiple agents can work methodically without silently changing the scientific design.

Every packet must return:

```text
Scope completed
Files changed
Tests added/updated
Commands/tests run
Evidence
Known gaps
Human/local dependencies
Recommended next packet
```

An agent must not claim `DONE` if the named acceptance gate was not executed.

---

## WP-R0 — Freeze the current CPV-2010 oracle

### Goal

Make the present 2010 target-year sampler an explicit regression oracle before refactoring.

### Read first

```text
censo_sampler/target_year.py
censo_sampler/streaming_target_year.py
tests/test_target_year_sampling.py
tests/test_streaming_target_year.py
docs/TARGET_YEAR_HOUSEHOLD_SAMPLING.md
```

### Tasks

- identify the exact fixture inputs used for 2024 and 2025;
- capture semantic fingerprints for selected source household IDs, selected source person IDs, selection probabilities and donor/target masses;
- do not change the algorithm;
- add regression helpers/tests if needed.

### Acceptance

- current tests remain green;
- 2024 and 2025 oracle semantics are machine-readable;
- no source microdata added to Git beyond existing fixture.

### Stop rule

Do not begin the new frame implementation in this packet.

---

## WP-R1 — Implement `research.census-frame/v1` datamodel

### Goal

Define the vintage-neutral donor-frame contract in code.

### Suggested files

```text
censo_sampler/frame_contract.py
censo_sampler/frame_validate.py
tests/test_frame_contract.py
```

### Contract

Minimum household index:

```text
frame_household_id
frame_dwelling_id
department_id
radio_id
household_person_count
```

Donor mass:

```text
department_id
donor_person_mass
```

Payload metadata for:

```text
vivienda.parquet
hogar.parquet
persona.parquet
```

Manifest must include Census vintage and frame/source identity.

### Tests

- duplicate household ID;
- missing dwelling FK;
- non-positive/invalid household size;
- donor mass mismatch;
- missing payload artifact;
- artifact hash mismatch;
- invalid Census vintage;
- valid 2010-shaped fixture;
- valid 2022-shaped fixture.

### Acceptance

No `P02`, `P03`, `EDAD`, EPH or RedEngine assumptions appear in the generic contract.

---

## WP-R2 — CPV-2010 CSV → full Parquet frame builder

### Goal

Turn the current authorized-style 2010 source layout into `research.census-frame/v1`.

### Reuse

Reuse validation ideas from `donor_frame.py`; do not rewrite graph validation from scratch without reason.

### Required behavior

Input:

```text
VIVIENDA.csv
HOGAR.csv
PERSONA.csv
GEOGRAPHY.csv
```

Output:

```text
manifest.json
frame_households.parquet
donor_person_mass.parquet
payload/vivienda.parquet
payload/hogar.parquet
payload/persona.parquet
```

### Rules

- preserve all source columns in payload Parquet;
- add neutral frame IDs/geography additively;
- derive household person counts from actual PERSONA membership;
- donor mass must equal total person count;
- source hashes/provenance mandatory;
- use streaming/disk-backed methods appropriate for national scale;
- do not require EPH-selected columns.

### Fixture acceptance

- exact row-count preservation;
- exact source-ID uniqueness/FKs;
- deterministic frame release ID;
- donor mass equality;
- repeat build either returns the same immutable release or fails according to the agreed immutable contract.

### Local deferral

National authorized 2010 run is human/local; fixture implementation is not.

---

## WP-R3 — Neutral target-population adapter

### Goal

Consume the existing governed parent without rewriting its source artifact.

### Implement

Adapter:

```text
department_2010_id -> department_id
```

Expose:

```text
department_id
target_year
target_person_mass
```

### Policy

Record:

```text
department_alignment_policy = assume-code-identity/v1
```

### Diagnostics

Before sampling, compute:

```text
matched departments
frame-only departments
target-only departments
```

### Acceptance

- existing governed parent bytes remain unchanged;
- 2010 fixture works;
- artificial mismatch fixture fails visibly;
- no automatic fuzzy/code remapping.

---

## WP-R4 — Extract the shared selection kernel

### Goal

Move sampling science out of raw Census I/O.

### Suggested file

```text
censo_sampler/selection.py
```

### Input

Only:

```text
frame_release_id
frame household index
donor person mass
target population
fraction
seed
bound policy
```

### Output

Selected household keys with:

```text
frame_household_id
department_id
selection_probability
design_inverse_probability_weight
```

### Score

Implement frame-aware common score:

```text
score(seed, frame_release_id, frame_household_id, department_id)
```

Target year must not enter the score.

### Required tests

- deterministic rerun;
- row-order independence;
- same score under 2024 vs 2025 for same frame;
- different frame release changes namespace/score input as specified;
- formula equality with current target-year method;
- overflow fails closed;
- no substantive Census variable names imported.

---

## WP-R5 — Prove CPV-2010 semantic parity

### Goal

Demonstrate that the new frame + kernel reproduces current sampling decisions.

### Compare

For both 2024 and 2025:

```text
current streaming_target_year.py
vs
new 2010 frame + selection.py
```

### Require exact parity for

- selected source/frame household IDs;
- selected source/frame person membership;
- probability by household;
- donor mass by department;
- target mass consumed;
- overflow behavior.

### Allowed difference

New v2 sample IDs may differ because namespace is intentionally frame-aware.

### Acceptance

Parity tests are mandatory CI gates.

### Stop rule

If parity fails, diagnose before continuing. Do not “update expected values” without explaining the scientific difference.

---

## WP-R6 — Canonical selection + membership artifacts

### Goal

Make selection itself a persistent, auditable artifact.

### Produce

```text
selection.parquet
person_membership.parquet
```

### Invariants

- one row per selected household;
- selected household IDs unique;
- every selected person belongs to exactly one selected household;
- every selected household has all source members;
- no substantive feature projection required.

### Tests

- duplicate key rejection;
- orphan person rejection;
- missing member detection;
- deterministic logical fingerprint.

---

## WP-R7 — Generic full-payload materializer

### Goal

Materialize complete relational sample tables after the selection is fixed.

### Implement

```text
selected keys
 -> payload/hogar.parquet
 -> payload/persona.parquet
 -> payload/vivienda.parquet
```

### Rules

- explicit key joins only;
- preserve all original payload columns;
- retain frame IDs/system fields;
- do not flatten three entities into one table;
- use Arrow/Parquet/DuckDB or another bounded-memory path;
- do not make feature choice part of sample identity.

### Modes

Implement or prepare for:

```text
selection-only
full-payload
```

### Acceptance

Fixture selected payload must equal filtering the original frame payload by the selected key sets.

---

## WP-R8 — Implement `research.census-target-year-sample/v2`

### Goal

Create a vintage-neutral release contract without mutating v1.

### Suggested files

```text
censo_sampler/release_v2.py
censo_sampler/validate_v2.py
```

### Required fields

Neutral:

```text
frame_vintage
frame_release_id
department_id
radio_id
sample_household_id
sample_person_id
selection_probability
design_inverse_probability_weight
```

### Identity

Sample IDs must include/derive from frame release namespace and must not be hard-coded to `cpv2010:`.

### Artifacts

```text
manifest.json
qa.json
selection.parquet
person_membership.parquet
vivienda.parquet
hogar.parquet
persona.parquet
```

### Checker

Must detect:

- payload corruption;
- selection/member mismatch;
- wrong frame manifest identity;
- FK failure;
- invalid probability semantics;
- forbidden generic `analysis_weight` invention.

---

## WP-R9 — Frame-based CLI

### Goal

Make prepared frames the recommended public surface.

### Add conceptual commands

```bash
censo-sampler frame build-2010 ...
censo-sampler frame check <frame>
censo-sampler sample --frame <frame> ...
censo-sampler check-release <release>
```

### Rules

- keep historical CLI functional where practical;
- do not make `sample` inspect raw RXDB;
- do not require RedEngine;
- errors should identify whether failure belongs to frame, target parent, sampling design or release materialization.

---

## WP-R10 — Build a CPV-2022-shaped synthetic frame fixture

### Goal

Prove source-vintage neutrality before waiting for the real RXDB extract.

### Fixture characteristics

Use 2022-style IDs and geography fields consistent with the expected adapter output.

Include:

```text
vivienda/hogar/persona relational keys
department_id
radio_id
multiple households
multiple persons per household
extra substantive columns unknown to the sampler
```

### Acceptance

The same `selection.py` and v2 release pipeline work without any `if vintage == 2022` branch.

---

## WP-R11 — Define 2022 frame handoff with `argentina-censo2022-rxdb`

### Goal

Specify exactly what the upstream adapter must emit for the sampler.

### Preferred owner

The actual 2022 frame builder belongs in `argentina-censo2022-rxdb`.

### Map

Expected conceptual mapping:

```text
hogar_key -> frame_household_id
persona_key -> frame_person_id
vivienda_key -> frame_dwelling_id
dpto_cmpcode -> department_id
radio_cmpcode -> radio_id
```

### Deliverables in sampler repo

- frame contract fixture;
- compatibility checker tests;
- handoff documentation.

### Local gate

Real `061471101` frame test waits only for the local RedEngine-qualified extract.

---

## WP-R12 — Real bounded CPV-2022 proof

### Dependency

Human/local operator provides validated `061471101` relational Parquet from the upstream extractor/adapter.

Expected source laboratory:

```text
73 VIVIENDA
56 HOGAR
137 PERSONA
```

### Tasks

- build/check frame;
- run shared sampler;
- materialize full sample;
- validate v2 release;
- compare no source-specific branches were introduced.

### Acceptance

Same library/CLI entry point as 2010.

---

## WP-R13 — README / SYSTEM / lifecycle retrofit

### Goal

Update repository truth only after implementation gates pass.

### README

Explain:

```text
2010 source -> prepared frame
2022 source -> prepared frame
same sampler
```

### SYSTEM.yaml

Change consumed authority from only local CPV-2010 microdata to governed `research.census-frame/v1` while retaining source-preparation compatibility responsibilities as appropriate.

### Lifecycle

Record v1 compatibility and v2 migration status.

### Do not overclaim

Do not say “supports CPV-2022” until either the real bounded 2022 gate or an explicitly described synthetic-only state has passed.

---

## WP-R14 — Downstream migration note

### Goal

Prepare, but do not execute, the downstream retrofit.

Document for `eph-censo-aligner` and `encuestador-de-hogares`:

- new v2 neutral IDs/geography;
- frame vintage metadata;
- all source columns retained;
- substantive feature alignment intentionally downstream;
- current consumer assumptions that reject non-2010 frames.

No EPH mapping work belongs in this packet.

---

# Recommended execution order

Critical path:

```text
R0
 ↓
R1
 ↓
R2 ─────┐
 ↓      │
R3      │
 ↓      │
R4      │
 ↓      │
R5      │
 ↓      │
R6      │
 ↓      │
R7      │
 ↓      │
R8      │
 ↓      │
R9      │
 ↓      │
R10     │
 ↓      │
R11 <───┘
 ↓
R12 [local data gate]
 ↓
R13
 ↓
R14
```

Parallelizable after the frame contract R1 freezes:

```text
R2 2010 frame builder
R3 target-population neutral adapter
R4 selection kernel
R10 synthetic 2022 fixture
```

Do not parallelize competing changes to the sample identity formula or manifest schema.

---

# Global agent rules

1. Preserve current tests and old v1 behavior until explicit parity/deprecation gates.
2. Never silently narrow the Census payload to model-selected columns.
3. Never let source row ordering become identity.
4. Never join blocks/tables positionally where explicit IDs exist.
5. Never make the sampler depend on RedEngine or RXDB.
6. Never infer missing geography mappings silently.
7. Never convert sampling probabilities into an `analysis_weight` without an approved consumer contract.
8. Keep real Census data outside Git.
9. Prefer small fixture-backed PRs over one migration mega-commit.
10. Every contract change must include tests and migration implications.

# Final autonomous-agent success condition

Agents may consider the workstream complete only when:

> A CPV-2010 frame and a CPV-2022 frame can both be passed to the same frame-based sampler entry point, which uses the same scientific selection code and emits the same v2 release contract, while source-specific differences are confined to frame construction and downstream feature alignment.
