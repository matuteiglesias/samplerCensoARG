# Census Sampler vintage-neutral retrofit plan

Status: **development plan**  
Target repository: `matuteiglesias/samplerCensoARG`  
Primary goal: make the governed sampler operate on either Argentina CPV-2010 or CPV-2022 through one shared sampling framework, without making the sampler own Census feature selection or Redatam extraction.

---

## 1. Executive objective

The mature sampler should no longer mean:

> “sample households from three CPV-2010 CSV files.”

It should mean:

> “sample households from a validated Argentina Census donor frame.”

The donor frame may come from:

- CPV-2010 CSV microdata converted once to a governed Parquet frame; or
- CPV-2022 relational Parquet produced by `rxdb-extractor` + `argentina-censo2022-rxdb`.

From the sampler onward, the same household-selection algorithm, target-year probability model, deterministic score, QA logic and release semantics should be used for both Census vintages.

The core migration is therefore an **input-contract retrofit**, not a new sampling method.

---

## 2. Scientific invariants that must survive unchanged

The retrofit MUST preserve these current scientific decisions:

1. **Household is the sampling unit.**
2. **Every member of a selected household is retained.**
3. **Target-year department composition is introduced through household selection probabilities**, not through a post-sampling calibration layer.
4. For donor frame person mass `D[d]`, target person mass `T[d,y]` and global intensity `c`:

   ```text
   p[d,y] = c * T[d,y] / D[d]
   ```

   subject to an explicit probability-bound policy.
5. **The deterministic household score is independent of target year**, so 2024 and 2025 use the same random ordering within one donor frame.
6. `selection_probability`, `design_inverse_probability_weight`, and any future `analysis_weight` remain distinct concepts.
7. Probability overflow remains fail-closed unless a separately approved policy is introduced.
8. Sample releases remain immutable, content/provenance-addressed and verifiable offline.
9. The sampler does not claim that a donor Census record becomes a contemporaneous observation merely because it is used for a later target year.

No work packet may “simplify” these invariants away.

---

## 3. New architecture

```text
                   SOURCE / PREPARATION

      CPV-2010                               CPV-2022
         │                                      │
 VIVIENDA/HOGAR/PERSONA CSV                RXDB / RBFX
         │                                      │
         │                               rxdb-extractor
         │                                      │
         │                         argentina-censo2022-rxdb
         │                                      │
         ▼                                      ▼
  CPV-2010 frame builder              CPV-2022 frame builder
         │                                      │
         └─────────────────┬────────────────────┘
                           ▼

                research.census-frame/v1

              narrow sampling/index plane
                     +
               full relational payload

                           │
                           ▼

                    samplerCensoARG

              one shared sampling kernel
                           │
                           ▼

                  selection artifact
                           │
                           ▼

                generic materializer
                           │
                           ▼

       research.census-target-year-sample/v2

                           │
                           ▼

             eph-censo-aligner / consumers
```

The important boundary is:

- frame builders know how to normalize a Census source into the donor-frame contract;
- the sampler knows only the donor-frame contract and sampling design;
- downstream consumers choose/align substantive Census variables.

---

## 4. New canonical input contract: `research.census-frame/v1`

The sampler MUST consume a prepared donor frame rather than raw Census files.

A frame consists of two conceptual planes.

### 4.1 Sampling/index plane

This plane is intentionally narrow.

Minimum household index:

```text
frame_household_id
frame_dwelling_id
department_id
radio_id
household_person_count
```

Minimum donor-mass table:

```text
department_id
donor_person_mass
```

Optional geography fields MAY include province or other source geography, but sampling MUST NOT depend on unrelated Census characteristics.

### 4.2 Payload plane

The frame carries the complete relational Census payload available to the local source preparation pipeline:

```text
payload/
  vivienda.parquet
  hogar.parquet
  persona.parquet
```

The payload SHOULD preserve all source Census columns rather than selecting only the variables historically needed by an EPH experiment.

Generated frame identity/geography columns are additive. Original Census variable names should remain intact unless a separate source adapter explicitly documents a normalization.

### 4.3 Frame manifest

At minimum:

```text
contract = research.census-frame/v1
country = ARG
census_vintage = 2010 | 2022
frame_release_id
source_release_id
source hashes / parent manifest hashes
geography policy
row counts
payload schema hashes
payload artifact hashes
sampling-index artifact hashes
```

The manifest MUST identify the exact donor frame used by every sample release.

---

## 5. Do not let the sampler choose Census variables

This is a normative design decision.

The current 2010 paths contain historical EPH-driven assumptions such as `P02`, `P03`, sex, age and other selected columns. These should not define the new sampler boundary.

The new sampling kernel MUST NOT know about:

```text
P02
P03
EDAD
CONDACT
education variables
housing-quality variables
EPH mappings
```

unless one of those fields is intrinsically required for sampling itself.

For the current design, the only person-level quantity required by the sampler is household membership, from which household size and donor person mass are derived.

Substantive feature projection belongs downstream, primarily in `eph-censo-aligner` or another named consumer.

---

## 6. Key-first sampling and late materialization

The scientific sampling decision MUST be represented independently from the payload columns.

### 6.1 First produce selected household keys

Conceptual artifact:

```text
selection.parquet
```

Fields:

```text
sample_household_id
frame_household_id
department_id
selection_probability
design_inverse_probability_weight
selection_score or score digest if useful for audit
```

This artifact is the canonical result of the sampling algorithm.

### 6.2 Then derive complete person membership

Conceptual artifact:

```text
person_membership.parquet
```

Fields:

```text
sample_person_id
frame_person_id
sample_household_id
frame_household_id
```

Every person belonging to a selected household must appear exactly once.

### 6.3 Materialize payload only after selection

Use selected keys to perform explicit semi/inner joins against the full frame payload:

```text
selected household keys
   ├─> selected HOGAR rows
   ├─> selected PERSONA rows through household FK
   └─> selected VIVIENDA rows through selected dwelling IDs
```

Do not define selection by the set of columns requested by a downstream model.

### 6.4 Materialization modes

The implementation SHOULD eventually support at least:

```text
selection-only
full-payload
```

`selection-only` is useful for audit or consumers that want to join their own source representation.

`full-payload` is the normal research release mode.

A future optional column projection MAY exist as a storage optimization, but it must be explicitly downstream of the fixed selected keys.

---

## 7. CPV-2010 frame preparation

The existing authorized 2010 CSVs remain valid source inputs.

Create a one-time governed frame builder:

```text
VIVIENDA.csv
HOGAR.csv
PERSONA.csv
GEOGRAPHY.csv
      │
      ▼
validate IDs and graph
      │
      ▼
convert full source tables to Parquet
      │
      ▼
derive canonical frame IDs / geography
      │
      ▼
derive household_person_count
      │
      ▼
derive donor_person_mass by department
      │
      ▼
research.census-frame/v1
```

Reuse the strong ideas already implemented in `donor_frame.py`:

- exact source hashing;
- uniqueness checks;
- PERSONA → HOGAR → VIVIENDA → RADIO validation;
- donor person mass computation;
- local-only custody of source microdata;
- bounded-memory / disk-backed processing.

The new builder SHOULD write Parquet directly and SHOULD NOT require repeated CSV parsing on every sampling run.

### 7.1 2010 frame identity

Prefer neutral frame IDs in the prepared frame, while retaining source IDs as source columns.

For example:

```text
frame_dwelling_id
frame_household_id
frame_person_id
```

These may initially be derived deterministically from the source reference IDs.

Do not hard-code `cpv2010:` into the generic sampler core.

---

## 8. CPV-2022 frame preparation

`samplerCensoARG` MUST NOT open RXDB or depend on RedEngine.

The 2022 path is:

```text
RXDB
  ↓
rxdb-extractor
  ↓
argentina-censo2022-rxdb
  ↓
validated relational Parquet
  ↓
CPV-2022 census-frame builder
  ↓
research.census-frame/v1
```

Expected mapping is approximately:

```text
hogar_key      -> frame_household_id
persona_key    -> frame_person_id
vivienda_key   -> frame_dwelling_id

dpto_cmpcode   -> department_id
radio_cmpcode  -> radio_id
```

The frame builder may live in `argentina-censo2022-rxdb`, because it is source-adapter logic, not sampling logic.

The sampler should receive only the completed frame directory and its manifest.

---

## 9. Geography policy for the first shared implementation

Use a deliberately simple first policy.

### 9.1 Working assumption

Treat the official five-digit department/partido/comuna code as the shared `department_id` across:

- CPV-2010 frame;
- CPV-2022 frame;
- governed target-population parent.

This is an explicit approximation expected to work for the overwhelming majority of departments.

Record in manifests:

```text
department_alignment_policy = assume-code-identity/v1
```

### 9.2 Fail visibly on actual exceptions

Before a target-year run, compare department code sets.

Produce diagnostics:

```text
matched
frame_only
target_only
```

Do not silently drop unmatched departments.

For the first implementation, a mismatch MAY fail closed.

Later, add a tiny governed override/crosswalk for the small number of departments affected by splits or identifier changes.

Do not build a general historical boundary-harmonization system before actual exceptions are enumerated.

---

## 10. Neutral target-population consumption

The current governed target-population artifact uses `department_2010_id`. Preserve it for compatibility.

Add a reader/adapter that exposes the neutral internal schema:

```text
department_id
target_year
target_person_mass
```

The adapter can initially implement:

```text
department_id := department_2010_id
```

and declare `assume-code-identity/v1`.

Do not rewrite the already governed target-population source merely to rename one field.

---

## 11. Shared sampling kernel

Extract the scientific algorithm from the 2010-specific I/O implementation.

The kernel inputs should be conceptually:

```text
frame_release_id
frame_households
  frame_household_id
  department_id
  household_person_count

donor_person_mass[department_id]
target_person_mass[department_id, target_year]
fraction
seed
probability_bound_policy
```

The kernel output is selected household keys plus sampling metadata.

### 11.1 Score contract

The deterministic score should become frame-aware:

```text
score(seed, frame_release_id, frame_household_id, department_id)
```

Do NOT include target year.

Therefore:

- 2024 and 2025 from the same donor frame share a deterministic ordering;
- CPV-2010 and CPV-2022 are different donor universes and cannot accidentally share identity.

### 11.2 Formula

Keep:

```text
p[d,y] = fraction * target_person_mass[d,y] / donor_person_mass[d]
```

The kernel must continue to fail if `p > 1` under the current approved `fail_on_overflow` policy.

---

## 12. New sample release contract: `research.census-target-year-sample/v2`

Do not mutate v1 in place.

v2 should be vintage-neutral.

### 12.1 Required manifest semantics

```text
contract
release_id
frame:
  frame_release_id
  census_vintage
  source_release_id
  frame_manifest_hash

target_population_parent
sampling_target_year
selection algorithm
fraction
seed
probability formula
probability-bound policy
weight semantics
artifact hashes
QA
```

### 12.2 Neutral geography fields

Use:

```text
department_id
radio_id
```

instead of hard-coded:

```text
department_2010_id
radio_2010_id
```

Frame vintage belongs in the manifest and/or explicit metadata, not in every geography column name.

### 12.3 Sample identity

Replace hard-coded `cpv2010:` identities with a frame-aware namespace, for example conceptually:

```text
census:<frame-namespace>:household:<frame-household-id>
census:<frame-namespace>:person:<frame-person-id>
```

Exact textual formatting should be frozen once tests exist.

Identity must be:

- deterministic;
- unique across frame releases;
- independent of target year;
- stable across 2024/2025 runs from the same donor frame.

### 12.4 Payload layout

Recommended:

```text
release/
  manifest.json
  qa.json
  selection.parquet
  person_membership.parquet
  vivienda.parquet
  hogar.parquet
  persona.parquet
```

Keep the relational structure rather than emitting one giant flattened person-level table.

---

## 13. 2010 migration-equivalence gate

This is the most important safety test.

The existing CPV-2010 fixture and current streaming implementation are the oracle.

For both 2024 and 2025:

```text
existing 2010 streaming sampler
              vs
2010 frame -> new shared sampling kernel
```

Must produce the same scientific selection:

- same selected source/frame household identities;
- same selected person membership;
- same selection probability by household;
- same donor mass by department;
- same target mass consumed;
- same target-year common-score behavior;
- same overflow failures.

The v2 files do NOT need byte identity with v1 because schemas and namespaces deliberately change.

Agents must compare semantic content, not merely total counts.

No 2022 production integration should be declared complete until this parity gate passes.

---

## 14. 2022 shared-framework acceptance gate

Once the real 2022 RXDB M3 laboratory is available, build a small `research.census-frame/v1` from:

```text
RADIO 061471101
73 VIVIENDA
56 HOGAR
137 PERSONA
```

Then run the exact same sampling kernel used for the 2010 fixture.

Acceptance condition:

> There is no branch in the scientific sampling algorithm of the form `if census_vintage == 2022`.

Vintage-specific differences may exist only in frame construction and source metadata.

Test:

- frame validation;
- donor mass;
- deterministic selection;
- complete household membership;
- payload materialization;
- release manifest;
- offline release validation.

---

## 15. CLI target

The mature CLI should foreground frames.

Conceptual commands:

```bash
censo-sampler frame build-2010 \
  --databasepath /secure/cpv2010 \
  --geography /secure/GEOGRAPHY.csv \
  --output-root /frames

censo-sampler frame check /frames/<frame-release>

censo-sampler sample \
  --frame /frames/<frame-release> \
  --target-population /targets/<release> \
  --target-year 2024 \
  --fraction 0.01 \
  --seed 20260831 \
  --materialize full \
  --output-root /samples

censo-sampler check-release /samples/<release>
```

A CPV-2022 frame should already have been produced upstream by the 2022 adapter; the sampler need only consume/check it.

Keep historical CLI commands available during transition where practical, but document them as compatibility paths.

---

## 16. Proposed internal modules

Names are recommendations, not rigid requirements.

```text
censo_sampler/
  frame_contract.py
  frame_validate.py
  frame_2010.py
  population_parent_adapter.py
  selection.py
  materialize.py
  release_v2.py
  validate_v2.py
```

Existing modules remain until parity and migration tests prove replacements:

```text
donor_frame.py
release.py
target_year.py
streaming_target_year.py
```

Do not delete the old implementation early. Use it as a regression oracle.

---

## 17. Delivery milestones

### R0 — Freeze current oracle

Deliver:

- document current v1/target-year behavior;
- pin fixture outputs or semantic fingerprints;
- ensure current 2024/2025 streaming equivalence tests remain green.

Acceptance:

- no behavior change.

### R1 — Define `research.census-frame/v1`

Implement:

- frame manifest datamodel;
- household-index schema;
- donor-mass schema;
- payload metadata contract;
- validation function.

Use only synthetic fixtures initially.

Acceptance:

- invalid keys/FKs/counts/hashes fail closed;
- no Census-vintage-specific feature assumptions in contract.

### R2 — CPV-2010 frame builder

Implement full CSV -> Parquet preparation.

Acceptance:

- validates exact source graph;
- preserves full source columns;
- emits household index and donor mass;
- content-addressed frame release;
- repeat run is deterministic/idempotent according to contract;
- no raw source is copied into Git.

### R3 — Shared selection kernel

Extract deterministic target-year sampling from raw I/O.

Acceptance:

- consumes only frame index/mass + target population;
- no P02/P03 or substantive Census feature names;
- common household score across target years;
- overflow fails closed.

### R4 — 2010 parity

Run current and new algorithms on existing fixture for 2024/2025.

Acceptance:

- exact household-selection parity;
- exact person-membership parity;
- exact probability parity;
- expected identity namespace differences only.

This gate is mandatory.

### R5 — v2 selection artifact and materializer

Implement:

- `selection.parquet`;
- `person_membership.parquet`;
- full relational payload materialization.

Acceptance:

- all selected households represented exactly once;
- all and only persons belonging to selected households appear;
- all and only referenced dwellings appear;
- full source columns preserved;
- joins are explicit by frame keys.

### R6 — `research.census-target-year-sample/v2`

Implement manifest/QA/checker.

Acceptance:

- neutral vintage/geography fields;
- frame-aware IDs;
- artifact hashes;
- offline checker detects corruption and identity drift.

### R7 — Neutral target-population adapter

Wrap current `department_2010_id` parent as neutral `department_id`.

Acceptance:

- existing governed target-population bytes remain unchanged;
- code-set diagnostics emitted;
- assume-code-identity policy recorded;
- unmatched departments fail visibly.

### R8 — CPV-2022 frame handshake

Coordinate with `argentina-censo2022-rxdb`.

Define and test exact frame-export handoff.

Acceptance:

- small synthetic 2022-style fixture first;
- then real `061471101` frame when local RXDB qualification is available;
- frame checker passes without sampler-specific source hacks.

### R9 — 2010/2022 same-kernel proof

Run identical sampling API against one 2010 and one 2022 frame.

Acceptance:

- no vintage branch inside sampling kernel;
- same release contract;
- same validation surfaces;
- differences restricted to frame identity/source payload.

### R10 — CLI/documentation migration

Make frame-based API the recommended front door.

Acceptance:

- README reflects 2010 + 2022 support state accurately;
- old historical commands clearly labeled;
- examples use prepared frames;
- lifecycle/system authority updated.

### R11 — Downstream compatibility handoff

Prepare migration note for `eph-censo-aligner` and `encuestador-de-hogares`.

Do not implement full 2022 semantic alignment here.

Acceptance:

- v2 sample schema documented;
- exact downstream blockers enumerated;
- no sampler code starts renaming EPH/Census semantic variables.

---

## 18. Agent collision policy

Safe parallel work after R1 contract freezes:

```text
2010 frame builder
frame validator
selection kernel
v2 release validator
CLI/docs
2022 handoff fixture
```

Avoid parallel agents making overlapping edits to:

```text
selection algorithm
sample identity contract
frame manifest schema
v2 release manifest schema
```

until those contracts have a designated owner and passing tests.

One agent should own a contract change end-to-end rather than multiple agents independently “improving” it.

---

## 19. Required test matrix

### Pure tests

- deterministic frame release identity;
- key uniqueness;
- FK coverage;
- donor mass sums to person count;
- sample score determinism;
- sample score changes when frame release changes;
- score does not change with target year;
- overflow failure;
- neutral target-population adapter;
- department-set mismatch diagnostics;
- frame-aware sample IDs;
- payload semi-join correctness;
- release hash checker.

### CPV-2010 fixture tests

- old vs new 2024 household parity;
- old vs new 2025 household parity;
- old vs new person membership parity;
- probability parity;
- streaming/full-frame equivalence;
- CSV -> Parquet retains required source data and row counts.

### CPV-2022 synthetic tests

- 2022-style IDs/geography;
- all original payload columns survive;
- sampler kernel has no source-specific logic;
- selected household completeness.

### Real CPV-2022 bounded test

Deferred until upstream local qualification supplies the `061471101` extract.

---

## 20. Human/local-machine queue

Autonomous agents SHOULD proceed without waiting for these tasks.

Defer only work that truly requires local source data or human policy decisions.

Human/local tasks later:

1. build/validate the real complete CPV-2010 Parquet frame from the authorized CSVs;
2. compare full-source row counts/hashes with the metadata lock;
3. supply the real bounded CPV-2022 `061471101` relational extract once the RedEngine bridge is qualified;
4. later supply the full/canonical CPV-2022 frame;
5. enumerate actual 2010↔2022 department-code exceptions after code-set comparison;
6. approve any explicit geography overrides;
7. decide downstream/publication handling of sample payloads if necessary.

Agents must not invent source paths, source hashes or geography exceptions in advance.

---

## 21. Explicit non-goals of this retrofit

Do NOT use this workstream to:

- teach `samplerCensoARG` how to query RXDB;
- import RedEngine into the sampler;
- redesign the target-year statistical method;
- build a general geography-history engine;
- retrofit all of `eph-censo-aligner` now;
- choose EPH feature columns in the sampler;
- flatten all Census entities into one giant table;
- migrate tests into a separate testing repository yet;
- delete historical v1 code before parity is proven;
- publish source Census microdata.

---

## 22. Definition of Done for the whole retrofit

The retrofit is complete when all of the following are true:

```text
[ ] research.census-frame/v1 exists and is validated.
[ ] CPV-2010 CSVs can be prepared once into a full Parquet frame.
[ ] The sampling kernel contains no CPV-2010 variable assumptions.
[ ] Sample selection is represented first as household keys/probabilities.
[ ] Full VIVIENDA/HOGAR/PERSONA payload is materialized only after selection.
[ ] research.census-target-year-sample/v2 is vintage-neutral.
[ ] Current 2010 fixture sampling is scientifically identical under the new path.
[ ] A 2022 frame can be consumed without changes to the sampling algorithm.
[ ] 2010 and 2022 use the same CLI/library sampling entry point.
[ ] Department-code identity is explicit and mismatches are diagnosed.
[ ] Historical v1 remains available or has an explicit deprecation path.
[ ] Downstream 2022 semantic alignment is left to eph-censo-aligner.
[ ] CI covers both a 2010 and a 2022-shaped frame.
```

The strongest final proof is simple:

> Given two valid `research.census-frame/v1` directories—one built from CPV-2010 and one from CPV-2022—the sampler can run the same command and the same sampling code against either frame, producing the same `research.census-target-year-sample/v2` contract with no Census-vintage branch in the scientific kernel.
