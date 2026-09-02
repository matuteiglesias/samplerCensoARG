# CPV-2022 frame handoff into Census Sampler

Status: **software contract ready; real local frame pending upstream RedEngine qualification/materialization**

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

Use the already established VP laboratory:

```text
RADIO 061471101
73 VIVIENDA
56 HOGAR
137 PERSONA
```

The upstream adapter should build a `research.census-frame/v1` for this bounded extract and run:

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

## What is already proven without real data

CI contains a synthetic CPV-2022-shaped frame with:

- 2022-style relational composite IDs;
- `department_id` / `radio_id`;
- 2022-specific source columns including `EDAD` and an intentionally unknown field;
- complete household membership.

It passes the exact same frame validator, selection kernel, full-payload materializer and v2 release validator as CPV-2010.

The remaining gate is therefore real-source qualification/materialization, not another sampler architecture change.
