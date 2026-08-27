# Target-year household sampling semantics

## Decision

This repository owns the optional **target-year department-stratified sampling design** used to draw a Census-2010-derived household sample whose represented **person mass by department** approximates another year's population-by-department distribution.

This is a sampling concern. It is **not** a downstream population-calibration layer and it does not turn Census-2010 microdata into an observed population for the target year.

The primary sampling unit is the **household**. All persons belonging to a selected household are retained. Person-level independent sampling is not required by the current poverty/inference use case and would destroy household integrity that downstream welfare construction needs.

## Historical method recovered

The historical sampler loaded a population-by-department table, computed a relative department size against 2010, and changed the household sampling fraction by department:

```text
m[d, y] = population[d, y] / population[d, 2010]

p[d, y] = base_fraction * m[d, y]
```

subject to a valid probability bound.

A growing department therefore gives every Census-2010 household in that department a larger selection probability for target year `y`; a shrinking department gives it a smaller probability.

The durable idea is **target-year geographic person composition through the household sampling design**, not a generic post-sampling calibration framework.

## Why household sampling gives the intended person-mass target

Let household `h` in department `d` contain `n_h` Census-2010 persons and let `I_h` be the Bernoulli indicator that the household is selected with the common department probability `p[d,y]`.

The number of selected persons in department `d` is:

```text
S[d,y] = sum_h I_h * n_h
```

Because every person in a selected household is retained:

```text
E[S[d,y]]
  = p[d,y] * sum_h n_h
  = p[d,y] * population[d,2010]
  = base_fraction * population[d,y]
```

before probability capping or other explicit bounds.

So the design has a clean expectation at the **person** level even though the sampling unit is the household. This is the main reason not to independently sample persons for the present use case: household sampling preserves household composition and relations while still targeting department-level person mass in expectation.

The expected number of selected **households** is not, in general, proportional to the target-year number of households because the external target is population persons, not household totals. That distinction must remain explicit in QA and downstream estimands.

## Statistical interpretation beyond department population

Within a department, every Census-2010 household has the same selection probability under the basic design. Therefore household characteristics remain an unbiased random donor sample of that department, and every Census-2010 person also has the same marginal inclusion probability within the department because inclusion occurs through the household.

For sufficiently large samples, person-level and household-level characteristics should therefore remain statistically represented **according to their Census-2010 within-department joint distribution**. When the department mixture changes, national marginals for characteristics correlated with department may also change mechanically through that new mixture.

The target-year population-by-department input does **not** independently update, for example:

- within-department age structure;
- household-size distribution;
- education;
- employment;
- housing conditions;
- within-department spatial composition;
- any other joint or marginal distribution.

Those dimensions are donor-frame assumptions, not hidden target-year projections. Adequacy depends on sample size, clustering by household, temporal stability, and the downstream question. Release QA should characterize realized balance rather than claim that all dimensions have been contemporaneously calibrated.

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
target_mass_unit = person
selection_unit = household
```

The selected household and person identities remain Census-2010-derived research identities.

## Weight semantics

Target-year composition and inverse-probability weighting must not be conflated.

If selection probability is deliberately changed by department to create a synthetic target-year person distribution, applying `1 / selection_probability` as the downstream analysis weight would approximately undo that geographic rebalancing and recover the donor-frame composition instead.

Therefore every release must distinguish at least:

- `selection_probability`: probability used to select the household and therefore each member of that household;
- `design_inverse_probability_weight`: optional audit/design quantity for inference back to the Census-2010 donor frame;
- `analysis_weight`: the weight, if any, authorized for the declared downstream estimand.

For a target-year **person-level** estimand, the realized selected persons are already geographically rebalanced in expectation by the sampling design. For a **household-level** estimand, the target-year person population does not by itself define target-year household totals, so additional weighting semantics must not be invented silently.

The exact downstream weighting rule must be declared by the consumer contract rather than inferred from a generic `sample_weight` column.

This is a required repair to the current governed release interface, which still exposes one overloaded weight field.

## QA implications

A target-year release should compare at least:

- target vs expected selected person counts by department;
- target vs realized selected person shares by department;
- realized household counts by department, explicitly labeled as household sample counts rather than target household totals;
- household-size distribution and selected-person counts as cluster-sampling diagnostics;
- selected-vs-donor distributions for important within-department variables where feasible;
- any department where probability capping changes the expectation above.

These are stochastic diagnostics. Ordinary finite-sample deviations are not contract failures unless a declared tolerance or structural invariant is violated.

## Non-claims

A target-year sample does **not** claim that:

- Census-2010 records were observed in the target year;
- within-department population composition is updated to the target year;
- the population-by-department source supplies household microdata or target-year household counts;
- target-year welfare, employment or demographic states are known before downstream inference;
- the sample is an official synthetic population or official population estimate.

It is a reproducible Census-2010 donor sample whose department-level **person mass** is informed by one declared target-year population source.

## Boundary with neighboring systems

```text
population-by-department source
             │
             ▼
      samplerCensoARG
  target-year household sampling
  (person mass target in expectation)
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
