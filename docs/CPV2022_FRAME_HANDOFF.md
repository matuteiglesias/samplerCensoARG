# CPV-2022 frame handoff into Census Sampler

Status: **real RPO-11 frame and bounded sampler handoff qualified in commissioning-only mode (2026-11-17); national scientific promotion still blocked**

## Ownership

The real CPV-2022 donor-frame producer belongs in:

```text
matuteiglesias/argentina-censo2022-rxdb
```

It consumes validated relational output from:

```text
matuteiglesias/rxdb-extractor
```

`samplerCensoARG` consumes only the resulting `research.census-frame/v1` directory.

The sampler must never:

- open `.rxdb`/`.rbfx` files;
- depend on RedEngine;
- know how NUMBER/FREQ extraction works;
- infer the 2022 hierarchy itself;
- select an EPH-oriented subset of Census variables.

## Expected mapping

For the VP database, the expected neutral mapping is:

```text
vivienda_key  -> frame_dwelling_id
hogar_key     -> frame_household_id
persona_key   -> frame_person_id

dpto_cmpcode  -> department_id
radio_cmpcode -> radio_id
```

The complete source payload should remain available as:

```text
payload/vivienda.parquet
payload/hogar.parquet
payload/persona.parquet
```

with the neutral frame IDs added, not replacing source variables.

`frame_households.parquet` is derived from HOGAR identity/geography plus exact PERSONA membership counts.

`donor_person_mass.parquet` is the sum of exact donor persons by `department_id`.

## First real acceptance fixture

The upstream adapter has already qualified real VP relational extraction on this laboratory:

```text
RADIO 061471101
73 VIVIENDA
56 HOGAR
137 PERSONA
```

The remaining cross-repository acceptance is to materialize the upstream adapter's `research.census-frame/v1` for this bounded extract and validate it independently with:

```bash
censo-sampler frame check /path/to/frame
```

Then, with a compatible one-department target-population fixture or parent subset, run the same public sampler command used for 2010:

```bash
censo-sampler sample \
  --frame /path/to/frame \
  --target-population /path/to/target \
  --target-year 2024 \
  --fraction <bounded value> \
  --seed 20260831 \
  --materialize full-payload \
  --output-root /path/to/samples
```

Acceptance requires:

- deep frame validation passes;
- same generic sampler entry point as 2010;
- no Census-vintage conditional in the scientific selection code;
- complete household membership;
- full selected VIVIENDA/HOGAR/PERSONA payload;
- source-only 2022 columns survive materialization;
- v2 release checker passes.

## Department geography

Initial policy:

```text
assume-code-identity/v1
```

Use official five-digit department/partido/comuna codes directly. Before a national target-year run the sampler will compare the frame and target code sets and fail visibly on unmatched codes.

Do not block the first 2022 frame on a general historical-geography project. Enumerate real exceptions first; then add a tiny explicit governed crosswalk if needed.

## What is already proven

Sampler CI contains a synthetic CPV-2022-shaped frame with:

- 2022-style relational composite IDs;
- `department_id` / `radio_id`;
- 2022-specific source columns including `EDAD` and an intentionally unknown field;
- complete household membership.

It passes the exact same frame validator, selection kernel, full-payload materializer and v2 release validator as CPV-2010.

Separately, `argentina-censo2022-rxdb` has real-qualified VP extraction on RADIO
`061471101` (73 VIVIENDA / 56 HOGAR / 137 PERSONA) and larger bounded slices,
plus an implemented Census-frame adapter.

The remaining gate is therefore one exact cross-repository bounded handoff:
upstream real frame bytes -> sampler independent frame check -> same target-year
sample/release path. It is not another sampler or extraction architecture project.


## Real RPO-11 acceptance and partial-coverage gate (2026-11-17)

The upstream `argentina-censo2022-rxdb` adapter (`frame-rpo11`, PR #11)
produced real `research.census-frame/v1` releases from immutable
SQLite-governed multi-RADIO Parquet shards. The full 3,000-rank window has
2,996 committed RADIOs, four explicitly excluded failures, 1,249,819 viviendas,
1,092,161 hogares and 2,451,927 personas. Independent deep frame check passed.
**This does not establish national or provincial representativeness.**

The identical bounded commissioning protocol also passed with the first
200 RADIOs (849 households / 1,729 persons selected) and the entire
3,000-ID window (2,996 committed RADIOs, 13 represented departments;
1,082 households / 2,392 persons selected). Both are `full-payload`
`research.census-target-year-sample/v2` releases with source masks and
explicit missing-coverage provenance intact. A separate 16-RADIO
`selection-only` release was materialized later with the same
commissioning restriction, validating preservation through that branch.

Local diagnostic outputs and executable evidence are recorded at
`Elements1/CENSO_work/benchmarks/rpo11-cpv2022-sampler-qualification-20261117/`.
All diagnostic target masses deliberately equal observed donor masses;
**none** is an official 2024 population projection.

A bounded real 16-RADIO frame was sampled with a SYNTHETIC diagnostic target
equal to the observed donor mass (NOT official population projections).
The full-payload v2 release contains 234 households and 464 persons, preserves
original Parquet source and `__mask` fields, and passes the independent
`censo-sampler check-release-v2` validator.

When the donor frame declares `source_coverage.coverage_is_partial=true`,
the normal planner and sampler now block even if department IDs align.
To exercise the partial frame for *software commissioning only*, explicitly
use `--commissioning-only`:

```bash
censo-sample-plan --frame <partial-frame> --target-population <diagnostic-target>   --target-year 2024 --fraction 0.05 --commissioning-only
censo-sampler sample --frame <partial-frame> --target-population <diagnostic-target>   --target-year 2024 --fraction 0.05 --seed 20260831   --materialize full-payload --commissioning-only --output-root <private-output>
censo-sampler check-release-v2 <private-output>/<release-id>
```

The release ID, v2 manifest and QA are tagged `commissioning_only`,
`publication_allowed=false`, with source-coverage provenance. The v2 checker
rejects missing commissioning markers if the sample declares partial coverage.
Passing this gate does not authorize statistical inference, target-year weights,
poverty publication or redistribution of person-level data.
