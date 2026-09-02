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

Record:

- frame release ID;
- source hashes;
- counts;
- runtime/elapsed time;
- disk usage;
- any schema/value surprises.

Expected outcome: a validated local `research.census-frame/v1`. No real Census payload should be committed to Git.

## H2 — Run one real v2 CPV-2010 sample

After H1, use the already governed target-population parent for 2024 and/or 2025.

Run the frame-based sampler and `check-release-v2`.

This is a scale/materialization qualification; fixture CI already proves scientific parity with the old streaming sampler.

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

## H5 — Run the same sampler on the bounded CPV-2022 frame

Use the same `censo-sampler sample --frame ...` interface as CPV-2010.

This is the real-data acceptance gate for claiming CPV-2022 support rather than synthetic compatibility only.

## H6 — Enumerate department-code exceptions

Before a national CPV-2022 target-year run, compare:

```text
CPV-2022 frame department_id set
vs
governed target-population department_id set
```

The current policy intentionally assumes code identity and fails closed on differences.

If a small set of split/renamed departments appears, record the exact exceptions. Only then design a tiny explicit governed crosswalk. Do not invent broad historical-geography machinery preemptively.

## H7 — National CPV-2022 frame and sample

Only after:

- upstream national RXDB extraction is qualified;
- bounded 2022 frame/sample gates pass;
- department exceptions are understood;
- disk/runtime budgets are known.

Materialize and deep-check the national 2022 frame, then run the desired 2024/2025 sample release(s).

## Deferred human decisions, not current blockers

- public redistribution policy for person-level Census-derived payloads;
- whether/when to introduce an explicit department-exception crosswalk;
- downstream `eph-censo-aligner` CPV-2022 semantic mapping and scientific feature choices;
- migration timing for downstream consumers that currently require sample-contract v1 / frame vintage 2010.

## Current software status

Everything before the real-data materialization gates is expected to remain automatable in CI. If an agent encounters a software task that can be tested with fixtures, it should implement it rather than adding it to this queue.
