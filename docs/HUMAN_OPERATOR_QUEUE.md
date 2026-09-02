# Human/local operator queue

This file contains only tasks that genuinely require local Census data, local runtime access, or a human decision. Ordinary software development should continue without waiting for these items.

## H1 — Materialize and deep-check the real CPV-2010 frame

Dependency: authorized complete local CPV-2010 CSV export.

Run conceptually:

```bash
censo-sampler frame build-2010 \
  --databasepath "$CENSUS_2010_DB_PATH" \
  --geography /path/to/GEOGRAPHY.csv \
  --output-root /secure/frames

censo-sampler frame check /secure/frames/<frame-release>
```

The build command itself now requires a successful deep relational validation before reporting success; the explicit second check remains useful as a detached verification command.

Record:

- frame release ID;
- source hashes;
- counts;
- runtime/elapsed time;
- disk usage;
- any schema/value surprises.

Expected outcome: a validated local `research.census-frame/v1`. No real Census payload should be committed to Git.

## H2 — Plan, then run one real v2 CPV-2010 sample

After H1, use the already governed target-population parent for 2024 and/or 2025.

First run without selecting/writing Census rows:

```bash
censo-sample-plan \
  --frame /secure/frames/<frame-release> \
  --target-population /secure/targets/<target-release> \
  --target-year 2024 \
  --fraction 0.01
```

If the plan is `ready`, run the frame-based sampler and `check-release-v2`.

This is a scale/materialization qualification; fixture CI already proves scientific parity with the old streaming sampler.

Record elapsed time, peak memory if convenient, output sizes, selected household/person counts and any department probability diagnostics.

## H3 — Produce the real bounded CPV-2022 relational extract upstream

Dependency: local RedEngine-qualified `rxdb-extractor` + `argentina-censo2022-rxdb` and local Census 2022 source.

First bounded source laboratory:

```text
RADIO 061471101
73 VIVIENDA
56 HOGAR
137 PERSONA
```

This work belongs upstream, not in the sampler.

## H4 — Materialize the bounded CPV-2022 Census frame

Using the upstream 2022 adapter, map the validated relational output into `research.census-frame/v1` as documented in `CPV2022_FRAME_HANDOFF.md`.

Then run:

```bash
censo-sampler frame check /path/to/cpv2022-frame
```

Expected: exact 73/56/137 source-universe preservation for the bounded VP extract, subject to the frame's household/person universe semantics.

## H5 — Plan, then run the same sampler on the bounded CPV-2022 frame

Use the same sampler semantics as CPV-2010.

First:

```bash
censo-sample-plan \
  --frame /path/to/cpv2022-frame \
  --target-population /path/to/compatible-target \
  --target-year 2024 \
  --fraction <bounded-value> \
  --details
```

Then, when ready:

```bash
censo-sampler sample --frame /path/to/cpv2022-frame ...
```

This is the real-data acceptance gate for claiming CPV-2022 support rather than synthetic compatibility only.

## H6 — Enumerate department-code exceptions

Before a national CPV-2022 target-year run, compare:

```text
CPV-2022 frame department_id set
vs
governed target-population department_id set
```

`censo-sample-plan --details` performs this comparison and reports:

```text
matched
frame_only
target_only
```

The current policy intentionally assumes code identity and blocks on differences.

If a small set of split/renamed departments appears, record the exact exceptions. Only then design a tiny explicit governed crosswalk. Do not invent broad historical-geography machinery preemptively.

## H7 — National CPV-2022 frame and sample

Only after:

- upstream national RXDB extraction is qualified;
- bounded 2022 frame/sample gates pass;
- department exceptions are understood;
- disk/runtime budgets are known.

Materialize and deep-check the national 2022 frame.

Before writing a national sample, run `censo-sample-plan` at the intended fraction/year and preserve its JSON output with the run notes. Only proceed if it reports `status=ready` or after an explicitly governed resolution of the reported blocker.

Then run the desired 2024/2025 sample release(s) and detached v2 validation.

## Deferred human decisions, not current blockers

- public redistribution policy for person-level Census-derived payloads;
- whether/when to introduce an explicit department-exception crosswalk;
- downstream `eph-censo-aligner` CPV-2022 semantic mapping and scientific feature choices;
- migration timing for downstream consumers that currently require sample-contract v1 / frame vintage 2010.

## Current software status

Everything before the real-data materialization gates is expected to remain automatable in CI. If an agent encounters a software task that can be tested with fixtures, it should implement it rather than adding it to this queue.

See `SAMPLE_PLAN.md` for the non-materializing planning contract and `IMPLEMENTATION_STATUS.md` for completed agent packets.
