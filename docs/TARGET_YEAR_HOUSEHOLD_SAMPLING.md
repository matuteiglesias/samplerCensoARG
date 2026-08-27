# Target-year household sampling semantics

## Decision

This repository owns the optional **target-year department-stratified sampling design** used to draw a Census-2010-derived household sample whose represented **person mass by department** approximates another year's population-by-department distribution.

This is a sampling concern. It is **not** a downstream population-calibration layer and it does not turn Census-2010 microdata into an observed population for the target year.

The primary sampling unit is the **household**. All persons belonging to a selected household are retained. Person-level independent sampling is not required by the current poverty/inference use case and would destroy household integrity that downstream welfare construction needs.

## Historical method recovered

The historical sampler loaded a population-by-department table, computed a relative department size against the table's 2010 column, and changed the household sampling fraction by department:

```text
legacy_multiplier[d, y] = projected_population[d, y] / projected_population[d, 2010]

legacy_selection_probability[d, y]
  = base_fraction * legacy_multiplier[d, y]
```

subject to a valid probability bound.

That implementation captured the durable idea — **target-year geographic person composition through the household sampling design** — but it unnecessarily coupled the target-year source to its own 2010 denominator.

The modern contract does not need that coupling.

## Modern design: donor mass and target mass are different authorities

For each department `d`, define:

```text
D[d]   = exact number of donor-frame persons in department d
         measured from the exact CPV donor frame

T[d,y] = target person population for department d and target year y
         from one exact governed population-by-department release

c      = global sampling intensity
```

The basic uncapped department household-selection probability is:

```text
p[d,y] = c * T[d,y] / D[d]
```

The donor denominator therefore comes from the **actual donor frame being sampled**, not from a demographic projection table's estimate for 2010.

This separation matters because it lets the sampler combine:

- an exact CPV-2010 donor frame;
- a later, independently versioned population-by-department source;
- an explicit target year;

without pretending that both sources share one demographic vintage or one estimation methodology.

The legacy formula is a special approximation to this design when the projection table's 2010 value is used as a proxy for the donor-frame person mass.

## Why household sampling gives the intended person-mass target

Let household `h` in department `d` contain `n_h` donor-frame persons and let `I_h` be the Bernoulli indicator that the household is selected with the common department probability `p[d,y]`.

The number of selected persons in department `d` is:

```text
S[d,y] = sum_h I_h * n_h
```

Because every person in a selected household is retained:

```text
E[S[d,y]]
  = p[d,y] * sum_h n_h
  = p[d,y] * D[d]
  = c * T[d,y]
```

before probability capping or other explicit bounds.

Therefore expected selected-person shares reproduce the declared target-year department population shares:

```text
E[S[d,y]] / sum_j E[S[j,y]]
  = T[d,y] / sum_j T[j,y]
```

This is the main reason not to independently sample persons for the present use case: household sampling preserves household composition and relations while still targeting department-level person mass in expectation.

The expected number of selected **households** is not, in general, proportional to the target-year number of households because the external target is population persons, not household totals. That distinction must remain explicit in QA and downstream estimands.

## Probability bounds are scientific state, not an implementation detail

If:

```text
c * T[d,y] / D[d] > 1
```

then the basic Bernoulli design cannot realize the requested intensity for that department without modification.

The governed implementation must fail closed or apply one explicitly named bounded design. It must never silently clip and continue as though the target expectation were unchanged.

At minimum the release must report:

- every department for which the uncapped probability exceeds one;
- the bound/policy applied;
- expected person mass before and after the bound;
- the resulting deviation from target department shares.

A future design may choose a lower global `c`, certainty strata, resampling, or another explicit policy. The contract should not preselect that methodological choice before a real consumer requires it.

## Statistical interpretation beyond department population

Within a department, every donor household has the same selection probability under the basic design. Therefore household characteristics remain a random donor sample of that department, and every donor person has the same marginal inclusion probability within the department because inclusion occurs through the household.

For sufficiently large samples, person-level and household-level characteristics should therefore remain statistically represented **according to their donor-frame within-department joint distribution**. When the department mixture changes, national marginals for characteristics correlated with department may also change mechanically through that new mixture.

The target-year population-by-department input does **not** independently update, for example:

- within-department age structure;
- household-size distribution;
- education;
- employment;
- housing conditions;
- within-department spatial composition;
- any other joint or marginal distribution.

Those dimensions are donor-frame assumptions, not hidden target-year projections. Adequacy depends on sample size, clustering by household, temporal stability, and the downstream question. Release QA should characterize realized balance rather than claim that all dimensions have been contemporaneously calibrated.

## Population-by-department source authority

The sampler owns **consumption and use** of a target population release. It does not own the demographic estimates themselves.

Repository archaeology recovered two committed legacy tables:

- `data/info/proy_pop200125.csv`, introduced in 2021, with a historical notebook comment pointing to INDEC's official 2010-2025 department-estimate publication;
- `data/info/proy_pop20012225.csv`, introduced in 2025, after which historical code switched to the new file while retaining the old provenance comment.

The second file therefore has **unresolved provenance in the repository** and must not become a governed parent merely because current legacy code reads it.

For a modern run, the exact parent must be pinned independently. Source families can differ by target period. For example, a current INDEC department-estimate family can govern later target years while the CPV-2010 donor count `D[d]` remains the denominator supplied by the donor frame. No target source is required to provide a synthetic `2010` denominator merely to fit the old formula.

## Required governed inputs

A scientific target-year run must pin:

- one exact Census donor-frame vintage (`2010` for the current design);
- one exact department geography identity/relation for the donor frame;
- exact donor-frame department person counts `D[d]` derived from that frame;
- one exact population-by-department target release supplying `T[d,y]`;
- the target year/date;
- global sampling intensity `c` or an equivalent named design parameter;
- deterministic seed and selection algorithm;
- the formula used to convert donor and target masses into household selection probabilities;
- probability-bound policy and diagnostics.

Historical committed `proy_pop*` CSVs are evidence of the old method, not automatically approved demographic authorities.

## Output semantics

The canonical sample release should keep separate fields/metadata for:

```text
frame_vintage
sampling_target_period
department_id
donor_person_mass
target_person_mass
relative_target_to_donor_mass
selection_probability_uncapped
selection_probability
probability_bound_policy
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
- `design_inverse_probability_weight`: optional audit/design quantity for inference back to the donor frame;
- `analysis_weight`: the weight, if any, authorized for the declared downstream estimand.

For a target-year **person-level** estimand, the realized selected persons are already geographically rebalanced in expectation by the sampling design. For a **household-level** estimand, the target-year person population does not by itself define target-year household totals, so additional weighting semantics must not be invented silently.

The exact downstream weighting rule must be declared by the consumer contract rather than inferred from a generic `sample_weight` column.

This is a required repair to the current governed release interface, which still exposes one overloaded weight field.

## QA implications

A target-year release should compare at least:

- donor person mass `D[d]` against the exact donor frame;
- target person mass `T[d,y]` against the exact population source release;
- target vs expected selected person counts by department;
- target vs realized selected person shares by department;
- realized household counts by department, explicitly labeled as household sample counts rather than target household totals;
- household-size distribution and selected-person counts as cluster-sampling diagnostics;
- selected-vs-donor distributions for important within-department variables where feasible;
- every department where probability bounds alter the uncapped expectation.

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
exact CPV donor frame ───────────────┐
  donor person mass D[d]             │
                                     ▼
exact population-by-department ─> samplerCensoARG
  target person mass T[d,y]       target-year household sampling
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
