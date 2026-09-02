# Downstream migration note — Census sample v2

Status: **handoff documentation only**

This document records what consumers such as `eph-censo-aligner` and `encuestador-de-hogares` must eventually change. The sampler intentionally does not implement those downstream semantic decisions.

## What changes in v2

The new release contract is:

```text
research.census-target-year-sample/v2
```

It is Census-vintage neutral.

Important manifest fields include:

```text
frame.frame_release_id
frame.census_vintage
frame.source_release_id
target_population_parent.target_year
materialization
```

Neutral geography replaces 2010-coded output names:

```text
department_id
radio_id
```

Sample identity is frame-aware rather than hard-coded to `cpv2010:`.

## What does NOT change scientifically

- household remains the sampling unit;
- every member of a selected household is retained;
- target-year department composition is implemented through household inclusion probability;
- design inverse-probability weights remain audit/design quantities rather than automatically approved model weights;
- the Census frame vintage remains distinct from sampling target year.

## Key-first artifacts

Consumers that need only sample identity can consume:

```text
selection.parquet
person_membership.parquet
```

without reading substantive Census columns.

`full-payload` releases additionally contain:

```text
vivienda.parquet
hogar.parquet
persona.parquet
```

with source Census columns preserved rather than projected into an EPH-specific subset.

## `eph-censo-aligner`

The aligner should become the owner of Census-vintage feature semantics.

Expected future behavior:

```text
Census 2010 source fields ─┐
                           ├─> common semantic feature plane
Census 2022 source fields ─┘
```

Many variable names appear compatible across vintages, but the aligner must explicitly own exceptions such as age field naming/coding and any changed universes/categories.

The sampler must not be modified merely because the aligner needs another variable. A `full-payload` sample already preserves all available source columns.

## `encuestador-de-hogares`

The current intake gate is known to require:

```text
research.census-target-year-sample/v1
frame vintage == 2010
department_2010_id
radio_2010_id
```

A future intake revision should:

1. recognize v2;
2. accept `frame.census_vintage` 2010 or 2022 subject to an approved semantic mapping;
3. consume neutral `department_id` / `radio_id`;
4. continue treating `selection_probability` and `design_inverse_probability_weight` as model-forbidden audit/design fields unless a downstream estimand explicitly authorizes otherwise;
5. require the EPH/Census semantic-plane mapping before model scoring.

Passing the v2 custody/intake gate should still **not** automatically authorize model scoring.

## Migration sequence

Recommended order:

```text
1. real CPV-2010 frame/v2 release
2. prove downstream v2 custody intake on 2010
3. bounded real CPV-2022 frame/v2 release
4. implement/approve CPV-2022 feature alignment
5. permit CPV-2022 model scoring
```

This sequence separates a data-contract migration from a scientific feature-mapping change.

## Explicit non-task for sampler agents

Do not add `if census_vintage == 2022` feature mappings, EPH aliases, education recodes, activity-status recodes or model-facing column projections to `samplerCensoARG`.

Those belong downstream.
