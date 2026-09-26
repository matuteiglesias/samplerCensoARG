# G2 handoff — EPH agglomerate identity on a fixed Census sample

This handoff attaches official EPH survey-geography identity to an already-built
`research.census-target-year-sample/v2` release.

It does not change the sampler.

## Inputs

1. one validated `research.census-target-year-sample/v2` release;
2. the exact official A7 release
   `arggeo.indec.eph.census2010.radio_frame`.

The sample must be based on the Census-2010 donor frame. A future Census-2022
sample requires its own governed relation and is rejected by this adapter.

## Join

```text
selection.radio_id
       LEFT JOIN
A7.frame.radio_2010_id
          ↓
eph_agglomerate_id nullable
```

Unmatched radios are retained as `outside_eph_frame`.

No selected household is removed. No household is moved to another radio. No
agglomerate is imputed.

## Outputs

```text
household_geography.parquet
population_frame_geography_patch.json
qa.json
manifest.json
checksums.sha256
```

The JSON patch is deliberately small and keyed by `sample_household_id`, so an
existing population-frame adapter can attach `eph_agglomerate_id` without
rebuilding or copying substantive Census payloads.

## QA

The handoff records:

- mapped and outside-EPH-frame household counts;
- mapped and outside selected-person counts;
- exact represented agglomerate IDs;
- per-agglomerate selected household/person counts;
- design-expanded person mass
  (`household_person_count * design_inverse_probability_weight`).

The design-expanded mass is an audit diagnostic inherited from the sampler. It
is **not** an agglomerate target-year calibration and is not automatically an
analysis weight.

## Scientific non-changes

The manifest asserts:

```text
sampling_changed = false
weights_changed = false
model_changed = false
poverty_region_added = false
agglomerate_population_calibration = false
redistribution_applied = false
imputation_applied = false
```

Thus `eph_agglomerate_id` is location metadata only.
