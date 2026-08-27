# Target-year household sampling semantics

## Decision

This repository owns the optional **target-year department-stratified sampling design** used to draw a Census-2010-derived household sample whose geographic composition approximates another year's department population distribution.

This is a sampling concern. It is **not** a downstream population-calibration layer and it does not turn Census-2010 microdata into an observed population for the target year.

The primary sampling unit is the **household**. All persons belonging to a selected household are retained. Person-level independent sampling is not required by the current poverty/inference use case.

## Historical method recovered

The historical sampler loaded a population-by-department table, computed a relative department size against 2010, and changed the household sampling fraction by department:

```text
m[d, y] = population[d, y] / population[d, 2010]

p[d, y] = base_fraction * m[d, y]
```

subject to a valid probability bound.

A growing department therefore contributes more Census-2010 households to the synthetic sample for year `y`; a shrinking department contributes fewer.

The durable idea is **target-year geographic composition through the sampling design**, not a generic post-sampling calibration framework.

## Statistical interpretation

For a sufficiently large household sample, the design relies on the working assumption that the Census-2010 households selected within each department remain a usable donor distribution for the joint characteristics that are not explicitly updated.

The target-year population-by-department information changes **department mass only**. It does not independently update, for example:

- age structure;
- household-size distribution;
- education;
- employment;
- housing conditions;
- within-department spatial composition;
- any other joint or marginal distribution.

Those dimensions are expected to remain statistically represented through sufficiently large random household sampling from the Census donor frame. This is a modeling/simulation assumption and must be declared as such in the release limitations.

## Required governed inputs

A scientific target-year run must pin:

- one exact Census source/frame vintage (`2010` for the current donor frame);
- one exact department geography identity/relation;
- one exact population-by-department source release;
- the target year/date;
- the base household sampling fraction;
- deterministic seed and selection algorithm;
- the formula used to convert target department populations into selection probabilities.

Historical committed `proy_pop*` CSVs are evidence of the old method, not automatically approved demographic authorities.

## Output semantics

The canonical sample release should keep separate fields/metadata for:

```text
frame_vintage
sampling_target_period
department_id
department_population_reference_2010
department_population_target
relative_department_size
selection_probability
selection_algorithm
population_source_release_id
```

The selected household and person identities remain Census-2010-derived research identities.

## Weight semantics

Target-year composition and inverse-probability weighting must not be conflated.

If selection probability is deliberately changed by department to create a synthetic target-year geographic composition, applying `1 / selection_probability` as the downstream analysis weight would approximately undo that geographic rebalancing and recover the donor-frame composition instead.

Therefore every release must distinguish at least:

- `selection_probability`: probability used to select the household;
- `design_inverse_probability_weight`: optional audit/design quantity for inference back to the donor frame;
- `analysis_weight`: the weight, if any, authorized for the declared downstream estimand.

For the target-year synthetic-sample use case, the analysis may intentionally use the realized sample composition rather than the donor-frame inverse-probability weight. The exact downstream weighting rule must be declared by the consumer contract rather than inferred from a generic `sample_weight` column.

This is a required repair to the current governed release interface, which still exposes one overloaded weight field.

## Non-claims

A target-year sample does **not** claim that:

- Census-2010 records were observed in the target year;
- within-department population composition is updated to the target year;
- the population-by-department source supplies household microdata;
- target-year welfare, employment or demographic states are known before downstream inference;
- the sample is an official population estimate.

It is a reproducible synthetic sampling frame whose department composition is informed by one declared target-year population source.

## Boundary with neighboring systems

```text
population-by-department source
             │
             ▼
      samplerCensoARG
  target-year household sampling
             │
             ▼
 research.census-sample/v1
             │
             ▼
   eph-census inference
             │
             ▼
      household welfare
             │
             ▼
         Poverty v2
```

`samplerCensoARG` owns the sampling design and its assumptions. It does not own welfare inference, poverty measurement, monetary semantics or demographic projection methodology itself.
