# Codex work packet — deterministic Census sample release v1

## Mission

Turn `samplerCensoARG` from a useful but weakly reproducible local sampler into a
bounded producer of immutable `research.census-sample/v1` releases that can be
consumed by `indice-pobreza-UBA`.

The immediate goal is one real local candidate sample when the CPV-2010 source
files are available. If the source files are unavailable, complete all fixture,
contract and readiness work and report the exact missing local requirement.

Do not commit raw Census microdata or a large generated sample to Git.

## Read first

Inspect:

- `README.md` and `LIFECYCLE.md`;
- `censo_sampler/cli.py`;
- `censo_sampler/io.py`;
- `censo_sampler/sample.py`;
- `censo_sampler/export.py`;
- `censo_sampler/validate.py`;
- `data/info/` reference files;
- historical notebooks only as evidence;
- the current poverty consumer contracts for `research.census-sample/v1`.

## Current issues that must be addressed

The current CLI describes the sample as reproducible, but the sampling path calls
`DataFrame.sample()` without a fixed random state.

The current `_sample_func()` also calculates:

```text
frac * department_projection_ratio
```

and catches sampling errors by returning an empty group. That can silently remove
a department, including when the effective fraction is greater than one.

The current run banner uses wall-clock time and host name, but there is no
content-addressed sample identity, source manifest, seed record or output QA.

Treat those as concrete defects, not merely documentation gaps.

# Part A — define the release boundary

Produce:

```text
research.census-sample/v1
```

with separate canonical tables:

## Persons

```text
sample_person_id
sample_household_id
sex_code
age_years
radio_2010_id
sample_weight
```

## Households

```text
sample_household_id
department_2010_id
region_id
sample_weight
```

Additional raw Census fields may be emitted as a separately declared optional
file. The poverty contract tables must remain small and explicit.

The manifest must declare:

- CPV-2010 source identity and local source hashes where feasible;
- source tables and required columns;
- selection algorithm and version;
- seed;
- requested sample fraction;
- realized household/person counts;
- geographic selection;
- requested analysis period;
- sample-ID namespace;
- weight and projection policies;
- producer commit and environment;
- files, sizes and SHA-256 hashes;
- QA, limitations and warnings.

# Part B — deterministic selection

Replace implicit pandas randomness with an explicit deterministic algorithm.

Preferred behavior:

- selection unit is household;
- every household receives a deterministic pseudo-random score derived from a
  cryptographic hash of `(seed, source household ID, selection stratum)`;
- selection is stable across row order, partitioning and worker count;
- all persons belonging to a selected household are retained;
- no housing/person orphan is possible;
- the algorithm is versioned in the manifest.

Do not depend on Dask partition order or `groupby.apply(...sample())` randomness.

Tests must prove:

- same source + seed + config yields the same selected IDs;
- different seed changes the sample;
- shuffled input order does not change selection;
- persons and households remain relationally complete;
- leading-zero geographic IDs are preserved as strings.

# Part C — separate sampling from projection

Do not hide population projection inside the sampling fraction.

Create two explicit concepts:

```text
frame_selection_probability
analysis_weight
```

For a basic frame sample:

```text
analysis_weight = 1 / frame_selection_probability
```

If the historical department-year projection ratios are retained for a candidate
product, represent them separately, for example:

```text
projected_analysis_weight = base_sample_weight * projection_factor
```

The release must state whether weights are:

- `cpv2010_frame_inverse_probability`;
- `legacy_department_projection_candidate`;
- or another explicitly named policy.

Never silently skip a department because an effective fraction exceeds one.
Choose and document one behavior:

- cap the inclusion probability at one and report a certainty stratum; or
- reject that requested configuration before sampling.

The recommended v1 behavior is to cap at one, retain all households in that
stratum and report the affected department/year cells as warnings. Do not return
an empty sample on a sampling exception.

# Part D — stable identifiers

Do not invent consumer IDs from row order.

Derive stable namespaced IDs from the source identifiers, preserving them in
manifested mappings:

```text
cpv2010:<release-namespace>:household:<source-household-id>
cpv2010:<release-namespace>:person:<source-person-id>
```

The exact namespace must include the source vintage, selection-policy version and
seed/config identity, but not the output path or wall-clock timestamp.

Verify uniqueness and foreign-key coverage before publication.

# Part E — geography and region contract

The sample must carry CPV-2010 radio and department IDs as zero-preserving strings.

Provide one deterministic region mapping compatible with the basket producer's
six regions:

```text
gran_buenos_aires
cuyo
noreste
noroeste
pampeana
patagonia
```

Do not map all Buenos Aires province to one region. Use the repository's existing
subprovincial/agglomerate evidence only where it is explicit. If a complete
Buenos Aires GBA-versus-Pampeana mapping is unavailable, either:

- resolve it from a pinned CPV-2010 geography/crosswalk release; or
- mark affected households unresolved and block a specifically requested poverty
  slice that needs them.

For a first bounded candidate, it is acceptable to select departments whose
region assignment is unambiguous, but that limitation must be explicit.

# Part F — fixture release and offline checks

Create compact synthetic CPV-like fixture tables covering:

- multiple persons per household;
- multiple departments and regions;
- one certainty stratum;
- leading-zero IDs;
- shuffled input order;
- duplicate source IDs;
- orphan persons;
- missing geography;
- invalid age/sex domains;
- projection factors below, equal to and above one.

Provide command surfaces equivalent to:

```bash
make check
make test
make sample-release-fixture
make sample-release-check RELEASE_DIR=...
```

The validator should use the standard library before pandas/Dask loads large
artifacts.

# Part G — build one real local candidate

Support a command resembling:

```bash
python -m censo_sampler.cli release \
  --databasepath "$CENSUS_DB_PATH" \
  --fraction 0.01 \
  --seed 20260804 \
  --analysis-period 2024-Q1 \
  --name ARG \
  --weight-policy legacy_department_projection_candidate \
  --output-root /local/releases/census
```

Adjust names to repository conventions, but preserve the explicit arguments.

The candidate target should be:

- nationwide when the source and region mapping support it;
- otherwise a bounded set of departments with complete geography/region support;
- sufficiently small for inspection and downstream iteration;
- large enough to include multiple departments and meaningful household
  structures.

Do not automatically choose 1% if memory/runtime makes it unreasonable. Report the
requested and realized scope and permit a smaller bounded fraction.

The candidate release should be copied—not linked by sibling path—into a local
handoff directory for `indice-pobreza-UBA`.

# Part H — QA and human-readable evidence

Emit machine-readable and readable reports containing:

- source table sizes and hashes;
- selected household/person counts;
- counts by department and region;
- requested versus realized inclusion probabilities;
- certainty strata;
- weight distribution summaries;
- duplicate/orphan/missing-geography counts;
- sex and age distributions;
- ID uniqueness and foreign-key coverage;
- release size and file hashes;
- exact limitations of projecting a 2010 Census frame to a later analysis period.

Add simple inspection plots locally, not to Git by default:

- households/persons by region;
- realized sample fraction by department;
- sample-weight distribution;
- age distribution.

# Hard failures versus warnings

Hard failures:

- missing required source tables or columns;
- duplicate source IDs that make stable IDs ambiguous;
- orphan persons after selection;
- unsafe output paths;
- checksum mismatch;
- nonfinite/nonpositive final sample weights;
- deterministic rerun selecting different IDs;
- unresolved region for a release that claims full basket compatibility.

Warnings:

- incomplete documentary provenance for local source bytes;
- projection policy retained only for historical compatibility;
- certainty strata caused by high projection factors;
- bounded department subset rather than nationwide coverage;
- source encoding or optional-column differences that do not affect the canonical
  contract.

# Non-goals

- No raw microdata commit.
- No public redistribution decision.
- No poverty calculation.
- No income inference.
- No EPH harmonization.
- No claim that projected weights create a current official population estimate.
- No scheduled or cross-repository mutation.

# Acceptance criteria

```text
sampling is deterministic across order and partition changes
seed/config/source identity is recorded
persons and households have stable namespaced IDs
weights and projection are separate declared concepts
no department disappears silently on an exception
canonical person/household tables satisfy the poverty consumer contract
a synthetic release is fully tested and checksum-verified
when CENSUS_DB_PATH is available, one real local candidate release is produced
otherwise the completion report states the exact missing local source requirement
```

# Completion report

The final PR must state:

- algorithm and seed behavior implemented;
- files and contracts added;
- fixture release identity;
- exact commands/tests run;
- real local candidate identity, scope, counts and output path when produced;
- departments/regions included and excluded;
- sample-weight policy and limitations;
- source/data rights limitations;
- downstream handoff instructions for `indice-pobreza-UBA`;
- confirmation that no raw Census microdata or large sample was committed.
