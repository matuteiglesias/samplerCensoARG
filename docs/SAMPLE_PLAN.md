# Non-materializing sample planning

Before a real CPV-2010 or CPV-2022 sample writes selected Census rows, run the dry planner:

```bash
censo-sample-plan \
  --frame /secure/frames/<frame-release> \
  --target-population /secure/targets/<target-release> \
  --target-year 2024 \
  --fraction 0.01
```

The command reads only frame custody/donor-mass metadata and the target-population parent. It does **not** select households and does **not** materialize Census payload rows.

A ready plan reports, among other fields:

```text
status = ready
frame_release_id
census_vintage
target_year
fraction
department_alignment
probability_range
probability_overflow_departments
total_donor_person_mass
total_target_person_mass
expected_selected_person_mass
```

Use `--details` for one row per department containing donor mass, target mass, computed selection probability and status.

The command exits nonzero when the plan is blocked, including:

- frame-only department codes;
- target-only department codes;
- nonfinite/invalid probabilities;
- selection probabilities above one under the current fail-on-overflow policy;
- target-parent custody failure.

This is the recommended human workflow:

```text
frame build/check
      ↓
censo-sample-plan
      ↓
inspect any geography/probability exceptions
      ↓
censo-sampler sample
      ↓
censo-sampler check-release-v2
```

The planner is advisory in the sense that it writes no sample, but its calculations use the same donor/target mass semantics as the production sampler. Production sampling still independently fails closed if the same conditions are violated.
