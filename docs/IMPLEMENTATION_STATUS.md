# Vintage-neutral retrofit implementation status

Date: 2026-09-02

This file maps `docs/CENSUS_VINTAGE_NEUTRAL_AGENT_PACKETS.md` to actual repository state so future agents do not repeat completed work or confuse local-data gates with missing software.

## WP-R0 — Freeze current CPV-2010 oracle

**DONE**

The historical `streaming_target_year.py` path remains intact and is used directly in parity tests for 2024 and 2025.

## WP-R1 — `research.census-frame/v1` datamodel

**DONE**

Implemented in `censo_sampler/frame_contract.py` with shallow custody validation and deep disk-backed relational validation.

## WP-R2 — CPV-2010 CSV → full Parquet frame builder

**SOFTWARE DONE / REAL NATIONAL MATERIALIZATION DEFERRED**

Implemented in `censo_sampler/frame_2010.py`.

Fixture proves:

- full source-column preservation;
- neutral frame IDs;
- exact relationships;
- household person counts;
- donor person mass;
- immutable source/artifact provenance.

The complete authorized local CPV-2010 corpus must still be materialized by the operator.

## WP-R3 — Neutral target-population adapter

**DONE**

Implemented in `censo_sampler/target_adapter.py`.

It adapts legacy `department_2010_id` parents to internal `department_id`, verifies declared parent payload hashes when available, and applies `assume-code-identity/v1` with fail-closed mismatch diagnostics.

## WP-R4 — Shared selection kernel

**DONE**

Implemented in `censo_sampler/selection.py`.

The migration intentionally preserves the historical deterministic score bytes:

```text
score(seed, frame_household_id, department_id)
```

Frame identity namespaces sample/release IDs instead of changing the pseudo-random selection. This is required for exact 2010 migration parity.

## WP-R5 — CPV-2010 semantic parity

**DONE / CI GATE**

`tests/test_frame_v2_parity.py` runs the old streaming sampler and the new frame-based sampler side by side for 2024 and 2025 and requires exact parity for selected source household IDs, source person IDs, person→household membership and household probabilities.

## WP-R6 — Canonical selection + membership artifacts

**DONE**

Sample v2 emits:

```text
selection.parquet
person_membership.parquet
```

before any substantive Census payload materialization.

## WP-R7 — Generic full-payload materializer

**DONE**

`full-payload` mode filters the complete relational donor payload by explicit selected keys and retains every source column.

`selection-only` mode omits substantive payload entirely.

## WP-R8 — `research.census-target-year-sample/v2`

**DONE**

Implemented in `censo_sampler/release_v2.py` with vintage-neutral identity/geography, explicit weight semantics, manifests, hashes, QA and offline checker.

## WP-R9 — Frame-based CLI

**DONE**

Modern commands:

```text
censo-sampler frame build-2010
censo-sampler frame check
censo-sampler sample
censo-sampler check-release-v2
```

Historical/v1 commands fall back to the original CLI.

## WP-R10 — Synthetic CPV-2022-shaped frame fixture

**DONE / CI GATE**

A synthetic 2022 frame passes the same validator, sampler, materializer and v2 checker as 2010. It contains 2022-specific fields including `EDAD` plus an intentionally unknown source column; both pass through without sampler-specific logic.

A separate fixture contains no sex/age/EPH-facing variables at all, proving those fields are no longer sampler requirements.

## WP-R11 — Define 2022 frame handoff

**DONE IN SAMPLER / UPSTREAM PRODUCER PENDING REAL DATA**

See `docs/CPV2022_FRAME_HANDOFF.md`.

The real producer belongs in `argentina-censo2022-rxdb` and maps validated RXDB relational output to `research.census-frame/v1`.

## WP-R12 — Real bounded CPV-2022 proof

**BLOCKED ONLY ON LOCAL DATA/RUNTIME**

Expected first gate:

```text
RADIO 061471101
73 VIVIENDA
56 HOGAR
137 PERSONA
```

The sampler-side software path is already implemented. Operator must provide the real frame produced upstream.

## WP-R13 — README / SYSTEM / lifecycle retrofit

**DONE**

README, `SYSTEM.yaml`, `LIFECYCLE.md`, Makefile and contract documentation now describe the frame/v2 architecture while preserving v1 compatibility truthfully.

## WP-R14 — Downstream migration note

**DONE AS DOCUMENTATION / DOWNSTREAM CODE DEFERRED**

See `docs/DOWNSTREAM_V2_MIGRATION.md`.

No EPH/Census feature mapping is implemented here by design.

---

# CI state

The target-year/frame workflow runs on Python 3.10 and 3.12 and exercises:

- historical governed target-year logic;
- historical streaming equivalence;
- governed target-population parent;
- donor-frame lock;
- frame/v2 contracts;
- old-vs-new CPV-2010 scientific parity;
- feature independence;
- synthetic CPV-2022 compatibility;
- row-order independence;
- frame/sample artifact tampering failures;
- target-parent custody;
- Makefile frame build/deep-check/sample/check operational targets.

# Remaining work category

The remaining critical-path work is no longer sampler architecture invention. It is **real-source materialization and qualification**:

```text
real CPV-2010 CSV → frame → v2 sample
real CPV-2022 RXDB → upstream relational extract → frame → same v2 sample
```

See `docs/HUMAN_OPERATOR_QUEUE.md` for the bounded keyboard-session work.
