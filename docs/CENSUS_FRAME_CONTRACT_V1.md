# `research.census-frame/v1`

Status: **implemented contract**

This is the canonical donor-frame input consumed by the vintage-neutral Census Sampler.

It deliberately separates **sampling/index information** from the **full source Census payload**. The sampler never needs to know which Census variables an EPH or other downstream model will use.

## Directory layout

```text
<frame>/
  manifest.json
  frame_households.parquet
  donor_person_mass.parquet
  payload/
    vivienda.parquet
    hogar.parquet
    persona.parquet
```

## Household index

`frame_households.parquet` contains exactly one row per donor household and at least:

```text
frame_household_id       string, unique, nonempty
frame_dwelling_id        string, nonempty
department_id             string, nonempty
radio_id                  string, nonempty
household_person_count    positive integer
```

It is intentionally narrow. No sex, age, education, employment, housing-quality or EPH-facing feature is part of the sampling contract.

## Donor person mass

`donor_person_mass.parquet` contains:

```text
department_id
 donor_person_mass
```

with one positive row per donor department.

For every department:

```text
donor_person_mass[d]
  == sum(frame_households.household_person_count for department d)
```

and nationally it equals the number of PERSONA rows.

## Payload

The payload is relational and preserves the complete source column set made available to the frame builder.

Required additive keys:

```text
payload/vivienda.parquet
  frame_dwelling_id

payload/hogar.parquet
  frame_household_id
  frame_dwelling_id

payload/persona.parquet
  frame_person_id
  frame_household_id
```

Original Census variables remain source-named. Feature projection and semantic alignment are downstream concerns.

## Referential invariants

The deep frame checker requires:

```text
PERSONA.frame_household_id -> HOGAR.frame_household_id
HOGAR.frame_dwelling_id    -> VIVIENDA.frame_dwelling_id
frame_households            == HOGAR household universe
household_person_count      == actual PERSONA membership count
```

IDs must be unique and nonempty.

## Manifest

Minimum semantic identity includes:

```text
contract = research.census-frame/v1
frame_release_id
country = ARG
census_vintage = 2010 | 2022
source_release_id
source/artifact hashes
counts
identity policy
department_alignment_policy
payload policy
```

The current department policy is:

```text
assume-code-identity/v1
```

meaning the official department/partido/comuna code is initially treated as the common sampling geography across CPV-2010, CPV-2022 and the target-population parent. Actual mismatches are surfaced and fail closed; a small governed exception mapping can be introduced later if required.

## CPV-2010 producer

Implemented in:

```text
censo_sampler.frame_2010.build_cpv2010_frame
```

It accepts the historical local CSV layout, validates the relational graph using disk-backed SQLite, preserves all CSV columns in Parquet, derives neutral keys and donor person mass, and produces an immutable content-addressed frame.

## CPV-2022 producer

The source-specific producer belongs upstream in `argentina-censo2022-rxdb`.

The sampler does **not** open RXDB and does not depend on RedEngine. A valid upstream frame only needs to satisfy this contract.

Expected source mapping is approximately:

```text
vivienda_key   -> frame_dwelling_id
hogar_key      -> frame_household_id
persona_key    -> frame_person_id
dpto_cmpcode   -> department_id
radio_cmpcode  -> radio_id
```

The exact real-data handoff is described in `CPV2022_FRAME_HANDOFF.md`.
