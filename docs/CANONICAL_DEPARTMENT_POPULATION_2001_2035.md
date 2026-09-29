# Canonical department population target surface, 2001–2035

## Purpose

This product gives the Census sampler one reproducible annual department-level
person-mass surface without pretending that one official demographic vintage
covers the whole 2001–2035 period.

It is a **research harmonization product**, not an official INDEC population
publication.

The output contract remains:

```text
publicdata.argentina-department-population-target/v1
```

and the method is:

```text
research.argentina-department-population-target/vintage-bridge-linear-v1
```

## Source policy

Three source roles are intentionally separated.

### 2001–2010: trusted legacy levels

`data/info/proy_pop20012225.csv` is treated as the trusted repository copy of
the historical INDEC department series for the 2001–2010 levels.

No revision is applied to these years.

### 2011–2021: legacy trajectory, level-bridged

The bridge uses the documented INDEC 2010–2025 projection family already
pinned in:

`data/info/proy_pop200125.csv`

This avoids treating later repository edits to the 2001+ convenience table as
a new demographic methodology. The two legacy files are required to reproduce
the same 2010 level for every department before any bridge is allowed.

For department (d), define:

```text
R_d = updated_2022[d] / legacy_2022[d]

alpha_y = (y - 2010) / (2022 - 2010)

scale_dy = 1 + alpha_y * (R_d - 1)

canonical_dy = round_half_up(legacy_dy * scale_dy)
```

for 2011–2021.

Therefore:

- the scale is exactly 1 in 2010;
- if extended to 2022, the bridged legacy value would equal the updated 2022
  value by construction;
- the actual 2022 row is not derived: it is taken directly from the updated
  official source.

The terminal ratio is **updated / legacy**. The inverse ratio would not land on
the updated 2022 level.

### 2022 onward: current INDEC estimates

The current source is INDEC's:

`base_estimaciones_pob_deptos_2022_2035.csv`

with metadata:

`metadatos_estimaciones_deptos_2022_2035.pdf`

and Análisis Demográfico N° 42.

INDEC describes these estimates as population at 1 July for departments,
partidos and comunas, based on Census 2022 and coherent with the current
national and jurisdiction projections.

The producer retains only the both-sex total rows.

## Geography

The product deliberately does **not** pretend that one department code system
is unchanged across the whole period.

- 2001–2021 rows retain the legacy source department ID.
- 2022+ rows retain the current source department ID.
- `geography_alignment.csv` records the relation used only to calculate the
  2022 terminal ratio for each historical series.
- direct code identity is preferred;
- otherwise a unique normalized same-name match within province is allowed;
- unresolved or ambiguous units fail closed unless an explicit override file
  is supplied.

This is important for cases such as CABA code changes and Tierra del Fuego.
Updated-only departments are emitted separately and are not given fabricated
pre-2022 histories.

## Value status

Each row declares one of:

```text
legacy_official_copy
derived_linear_vintage_bridge
direct_indec_2022_based_estimate
```

Downstream analyses can therefore keep official-source values separate from
the research bridge.

## Build

Fetch the current official CSV:

```bash
censo-sampler target-population fetch-updated \
  --output /home/matias/data/population-sources/base_estimaciones_pob_deptos_2022_2035.csv
```

Build the immutable canonical release:

```bash
censo-sampler target-population build-canonical \
  --updated-source /home/matias/data/population-sources/base_estimaciones_pob_deptos_2022_2035.csv \
  --output-root /home/matias/data/department-population-canonical
```

If the real source reveals an administrative identity that cannot be aligned
uniquely, provide a reviewed CSV:

```text
legacy_department_id,updated_department_id
...
```

using `--geography-overrides`.

## Release contents

```text
target_population.csv
geography_alignment.csv
updated_only_departments.csv
qa.json
manifest.json
```

The manifest binds all three source hashes, the interpolation method, coverage,
geography policy and artifact hashes.

## Sampler use

The modern frame-based sampler consumes `target_population.csv` through the
existing neutral target-population adapter.

The target population changes department-level person mass only. It does not
update within-department age, household structure, education, labor, housing or
other Census characteristics.

For years on different geography vintages, use the donor frame whose
department identities are compatible with that year's target-population rows.
Do not silently coerce a CPV-2010 frame to CPV-2022 department identities.

## Scientific claim boundary

The 2011–2021 bridge is intentionally simple. It is useful for approximate
target-year sampling mass and longitudinal research infrastructure, but it is
not a replacement for INDEC's demographic estimation methodology.

Its virtue is transparency: the original legacy trajectory is preserved, and
only a smooth department-specific level correction is introduced so that the
historical system joins the revised 2022 level without a discontinuous jump.
