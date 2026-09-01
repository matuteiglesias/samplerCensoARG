# INDEC 2010–2025 department population parent

## Purpose

The first governed target-year sampler baseline uses the INDEC department/partido/comuna population estimates for 2010–2025 as the external target-mass authority while retaining CPV-2010 microdata as the donor frame.

Only target years **2024 and 2025** are projected into the canonical parent consumed by the sampler. The donor denominator remains the exact person count measured from the CPV-2010 donor frame; the population table does not supply a synthetic donor denominator.

## Exact repository snapshot

The governed source bytes are:

```text
data/info/proy_pop200125.csv
Git blob SHA-1: 2bfa46dc782d70399b5b749e1d71e38887dec5ca
```

The builder recomputes Git's blob identity from the local bytes and fails if it differs. It also records SHA-256 and size in the emitted manifest.

The similarly named `data/info/proy_pop20012225.csv` is deliberately excluded from this baseline. Repository archaeology found that historical code later switched to that file while retaining the old provenance comment, leaving its exact source provenance unresolved.

## Public-source evidence

Historical notebook text identifies `proy_pop200125.csv` as the official INDEC department projection table and points to:

```text
INDEC (2015)
Estimaciones de población por sexo, departamento y año calendario 2010-2025
Serie Análisis Demográfico N° 38
```

Official publication:

```text
https://www.indec.gob.ar/ftp/cuadros/poblacion/proyeccion_departamentos_10_25.pdf
```

The publication states that its estimates are population at **1 July** of each calendar year and uses “departamento” generically for departments, Buenos Aires partidos, and CABA comunas.

Before governing the snapshot, bounded sentinels were checked against the official Buenos Aires table:

| 2010-era ID | Unit | 2024 | 2025 |
|---|---|---:|---:|
| `06028` | Almirante Brown | 612,438 | 616,000 |
| `06035` | Avellaneda | 360,583 | 361,532 |

The canonicalizer also preserves CABA code padding, e.g. source `2001` becomes `02001`.

These checks establish a defensible provenance/value link for the committed snapshot. They do **not** claim that the CSV is byte-identical to a table embedded in the PDF.

## Canonical parent

`censo_sampler.target_population.build_indec_2010_2025_target_parent` emits an immutable content-addressed parent:

```text
publicdata.argentina-department-population-target/v1
```

with canonical rows:

```text
department_2010_id
department_name
target_year
target_person_mass
```

and exactly two target years: 2024 and 2025.

The parent manifest records source bytes, publication identity, target clock, mass unit, geography basis, artifact hashes, QA, and limitations.

## Sampler binding

`build_target_year_release_from_parent(...)` validates the parent payload and derives the sampler's source identity from:

```text
<population release id>@manifest-sha256:<manifest digest>
```

The sampler therefore cannot silently consume one target CSV while claiming another source label.

Its sampling design remains:

```text
D[d]   = donor-frame persons in department d
T[d,y] = governed target persons in department d, year y
c      = global sampling intensity

p[d,y] = c * T[d,y] / D[d]
```

Households are selected with the same deterministic household score in both target years; only `p[d,y]` changes. Every member of each selected household is retained.

## Claim boundary

This baseline updates only department-level target person mass. Within-department age, household structure, education, labor, housing, and other joint distributions remain inherited from the CPV-2010 donor frame.

The INDEC publication is itself a small-area demographic estimation product based on 2001/2010 census information and provincial projections. Its methodological limitations carry into this parent. The resulting sample is a research donor sample, not an official reconstruction of 2024 or 2025 micro-population.

A Census-2022-based department series is a separate later sensitivity and requires an explicit geography relation before it can enter this design.
