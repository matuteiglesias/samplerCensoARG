# Candidate release runbook (2026-08-04)

## Local source readiness

The environment was searched for the three CPV table basenames. Only the
committed synthetic fixture was found; `CENSUS_DB_PATH` is also unset. A real
candidate therefore cannot be generated in this environment. The exact missing
local requirement is an authorized directory containing `VIVIENDA.csv`,
`HOGAR.csv`, and `PERSONA.csv`, plus a pinned local radio crosswalk with
`RADIO_REF_ID`, zero-preserving `radio_2010_id` and `department_2010_id`, and a
complete six-region `region_id` for the requested scope. No raw microdata was
copied into Git.

## Fixture identity and policy

Run `make sample-release-fixture`. With seed `20260804`, fraction `0.1`, period
`2024-Q1`, and the legacy candidate policy, the release identity is
`research.census-sample-v1-8e68678142111879d4af`. The fixture intentionally
contains multiple-person households, four departments/regions, leading-zero
geography, shuffled rows, projection factors below/equal/above one, and certainty
strata. The invalid fixture exercises ambiguous duplicate IDs and also records an
orphan person plus invalid sex/age values for hard-failure development.

The canonical `sample_weight` is inverse frame probability for the base policy.
For `legacy_department_projection_candidate` it is that base weight multiplied
by the separately declared projection factor. This compatibility policy does not
produce a current official estimate.

## Real candidate and handoff

After setting the secure paths, run the release command from the README. Use an
explicit unambiguous `--departments` subset if the Buenos Aires crosswalk is not
complete. The command validates duplicate IDs, orphans, geography, domains, and
weights before publication; caps explicit frame multipliers at certainty; and
refuses a frame above `--max-households`. The `--handoff-dir` destination receives
an immutable copied directory suitable for `indice-pobreza-UBA`. Verify it with:

```bash
python -m censo_sampler.cli check-release /local/handoffs/indice-pobreza-UBA/<release-id>
```

Inspection plots are deliberately local-only. From `qa.json`, plot
`households_by_region`, `department_sampling.realized_fraction`, weights from the
canonical tables, and `age_distribution`; do not commit those plots or releases.
