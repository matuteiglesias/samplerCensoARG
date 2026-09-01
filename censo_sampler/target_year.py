"""Governed target-year household sampling from a CPV-2010 donor frame.

This module is intentionally separate from the historical release path.  It
updates only department person mass through household selection probabilities:

    p[d, y] = fraction * target_person_mass[d, y] / donor_person_mass[d]

Households are the selection unit and every person in a selected household is
retained.  The deterministic household score does not include target year, so
2024 and 2025 releases use the same random ordering within each department.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import shutil
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

CONTRACT = "research.census-target-year-sample/v1"
ALGORITHM = "sha256-common-household-score/v1"
TARGET_YEARS = {2024, 2025}
SOURCE_FILES = ("VIVIENDA.csv", "HOGAR.csv", "PERSONA.csv")
HOUSEHOLD_FIELDS = [
    "sample_household_id",
    "department_2010_id",
    "selection_probability",
    "design_inverse_probability_weight",
]
PERSON_FIELDS = [
    "sample_person_id",
    "sample_household_id",
    "department_2010_id",
    "radio_2010_id",
    "sex_code",
    "age_years",
    "selection_probability",
    "design_inverse_probability_weight",
]


class TargetYearSamplingError(ValueError):
    """Hard failure in governed target-year sampling."""


def _canonical_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _dialect(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        sample = stream.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        return csv.excel


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return [
            {key: (value or "").strip() for key, value in row.items()}
            for row in csv.DictReader(stream, dialect=_dialect(path))
        ]


def _require(rows: list[dict[str, str]], columns: tuple[str, ...], table: str) -> None:
    present = set(rows[0]) if rows else set()
    missing = sorted(set(columns) - present)
    if missing:
        raise TargetYearSamplingError(
            f"{table}:missing_required_columns:{','.join(missing)}"
        )


def _unique(rows: list[dict[str, str]], key: str, table: str) -> None:
    counts = Counter(row.get(key, "") for row in rows)
    bad = sorted(value for value, count in counts.items() if not value or count > 1)
    if bad:
        raise TargetYearSamplingError(f"{table}:duplicate_or_empty_{key}:{bad[:5]}")


def _score(seed: int, household_id: str, department_id: str) -> float:
    # Deliberately no target year: the same donor household has the same score
    # in 2024 and 2025. Only p[d,y] changes.
    payload = f"{seed}\x1f{household_id}\x1f{department_id}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest(), "big") / 2**256


def _stable_id(unit: str, source_id: str) -> str:
    return f"cpv2010:{unit}:{source_id}"


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _target_mass(
    target_path: Path, target_year: int
) -> tuple[dict[str, int], dict[str, object]]:
    rows = _rows(target_path)
    _require(
        rows,
        ("department_2010_id", "target_year", "target_person_mass"),
        "TARGET_POPULATION",
    )
    selected: dict[str, int] = {}
    for row in rows:
        try:
            year = int(row["target_year"])
        except ValueError as exc:
            raise TargetYearSamplingError("TARGET_POPULATION:invalid_target_year") from exc
        if year != target_year:
            continue
        department = row["department_2010_id"]
        if department in selected:
            raise TargetYearSamplingError(
                f"TARGET_POPULATION:duplicate_department:{department}"
            )
        try:
            mass = int(row["target_person_mass"])
        except ValueError as exc:
            raise TargetYearSamplingError(
                f"TARGET_POPULATION:invalid_mass:{department}"
            ) from exc
        if mass <= 0:
            raise TargetYearSamplingError(
                f"TARGET_POPULATION:nonpositive_mass:{department}"
            )
        selected[department] = mass
    if not selected:
        raise TargetYearSamplingError(
            f"TARGET_POPULATION:no_rows_for_target_year:{target_year}"
        )
    return selected, {
        "sha256": _sha256(target_path),
        "size_bytes": target_path.stat().st_size,
    }


def build_target_year_release(
    databasepath: Path,
    output_root: Path,
    *,
    target_population_path: Path,
    target_source_id: str,
    target_year: int,
    fraction: float = 0.01,
    seed: int = 20260831,
    geography_path: Path | None = None,
    max_households: int = 100000,
) -> Path:
    """Build one deterministic target-year sample release.

    Probability overflow fails closed. This first governed mode never clips and
    never emits a generic ``sample_weight`` or ``analysis_weight`` field.
    """
    if target_year not in TARGET_YEARS:
        raise TargetYearSamplingError(
            f"target_year_must_be_one_of:{sorted(TARGET_YEARS)}"
        )
    if not (0 < fraction <= 1):
        raise TargetYearSamplingError("fraction_must_be_in_(0,1]")
    if max_households < 1:
        raise TargetYearSamplingError("max_households_must_be_positive")
    if not target_source_id.strip():
        raise TargetYearSamplingError("target_source_id_required")

    source = Path(databasepath).expanduser().resolve()
    missing = [name for name in SOURCE_FILES if not (source / name).is_file()]
    if missing:
        raise TargetYearSamplingError(
            "missing_cpv2010_source_files:" + ",".join(missing)
        )
    geo_path = (
        Path(geography_path).expanduser().resolve()
        if geography_path
        else source / "GEOGRAPHY.csv"
    )
    if not geo_path.is_file():
        raise TargetYearSamplingError("missing_geography_crosswalk")
    target_path = Path(target_population_path).expanduser().resolve()
    if not target_path.is_file():
        raise TargetYearSamplingError("missing_target_population_parent")
    output_root = Path(output_root).expanduser().resolve()
    if output_root == source or source in output_root.parents:
        raise TargetYearSamplingError("unsafe_output_path_inside_source")

    vivienda = _rows(source / "VIVIENDA.csv")
    hogares = _rows(source / "HOGAR.csv")
    personas = _rows(source / "PERSONA.csv")
    geography = _rows(geo_path)
    _require(vivienda, ("VIVIENDA_REF_ID", "RADIO_REF_ID"), "VIVIENDA")
    _require(hogares, ("HOGAR_REF_ID", "VIVIENDA_REF_ID"), "HOGAR")
    _require(
        personas,
        ("PERSONA_REF_ID", "HOGAR_REF_ID", "P02", "P03"),
        "PERSONA",
    )
    _require(
        geography,
        ("RADIO_REF_ID", "radio_2010_id", "department_2010_id"),
        "GEOGRAPHY",
    )
    _unique(vivienda, "VIVIENDA_REF_ID", "VIVIENDA")
    _unique(hogares, "HOGAR_REF_ID", "HOGAR")
    _unique(personas, "PERSONA_REF_ID", "PERSONA")
    _unique(geography, "RADIO_REF_ID", "GEOGRAPHY")

    viv_by_id = {row["VIVIENDA_REF_ID"]: row for row in vivienda}
    hh_by_id = {row["HOGAR_REF_ID"]: row for row in hogares}
    geo_by_radio = {row["RADIO_REF_ID"]: row for row in geography}
    missing_viv = sorted(
        {row["VIVIENDA_REF_ID"] for row in hogares} - set(viv_by_id)
    )
    orphan_people = sorted(
        {row["HOGAR_REF_ID"] for row in personas} - set(hh_by_id)
    )
    unresolved_radios = sorted(
        {row["RADIO_REF_ID"] for row in vivienda} - set(geo_by_radio)
    )
    if missing_viv or orphan_people or unresolved_radios:
        raise TargetYearSamplingError(
            "donor_relational_failure:"
            f"missing_viviendas={missing_viv[:3]}:"
            f"orphan_person_households={orphan_people[:3]}:"
            f"missing_radios={unresolved_radios[:3]}"
        )

    household_geo: dict[str, dict[str, str]] = {}
    for household in hogares:
        vivienda_row = viv_by_id[household["VIVIENDA_REF_ID"]]
        household_geo[household["HOGAR_REF_ID"]] = geo_by_radio[
            vivienda_row["RADIO_REF_ID"]
        ]

    donor_person_mass: Counter[str] = Counter()
    people_by_household: dict[str, list[dict[str, str]]] = defaultdict(list)
    for person in personas:
        household_id = person["HOGAR_REF_ID"]
        department = household_geo[household_id]["department_2010_id"]
        donor_person_mass[department] += 1
        people_by_household[household_id].append(person)

    target_person_mass, target_meta = _target_mass(target_path, target_year)
    donor_departments = set(donor_person_mass)
    target_departments = set(target_person_mass)
    if donor_departments != target_departments:
        raise TargetYearSamplingError(
            "target_department_coverage_mismatch:"
            f"missing={sorted(donor_departments-target_departments)}:"
            f"extra={sorted(target_departments-donor_departments)}"
        )

    probabilities: dict[str, float] = {}
    for department in sorted(donor_departments):
        donor_mass = donor_person_mass[department]
        target_mass = target_person_mass[department]
        probability = fraction * target_mass / donor_mass
        if not math.isfinite(probability) or probability <= 0:
            raise TargetYearSamplingError(
                f"invalid_selection_probability:{department}:{probability}"
            )
        if probability > 1:
            raise TargetYearSamplingError(
                f"selection_probability_overflow:{department}:{probability:.12g}"
            )
        probabilities[department] = probability

    frame = []
    for household in hogares:
        household_id = household["HOGAR_REF_ID"]
        geography_row = household_geo[household_id]
        department = geography_row["department_2010_id"]
        probability = probabilities[department]
        score = _score(seed, household_id, department)
        frame.append((household, geography_row, probability, score))
    if len(frame) > max_households:
        raise TargetYearSamplingError(
            f"bounded_release_limit_exceeded:{len(frame)}>{max_households}"
        )

    selected = [item for item in frame if item[3] < item[2]]
    if not selected:
        raise TargetYearSamplingError("deterministic_selection_empty")
    selected_ids = {household["HOGAR_REF_ID"] for household, _, _, _ in selected}
    selected_people = [
        person for person in personas if person["HOGAR_REF_ID"] in selected_ids
    ]
    if not selected_people:
        raise TargetYearSamplingError("selected_households_have_no_people")

    household_rows: list[dict[str, str]] = []
    household_lookup: dict[str, tuple[str, dict[str, str], float]] = {}
    for household, geography_row, probability, _ in selected:
        household_id = household["HOGAR_REF_ID"]
        sample_id = _stable_id("household", household_id)
        inverse_probability = 1 / probability
        household_lookup[household_id] = (
            sample_id,
            geography_row,
            probability,
        )
        household_rows.append(
            {
                "sample_household_id": sample_id,
                "department_2010_id": geography_row["department_2010_id"],
                "selection_probability": format(probability, ".17g"),
                "design_inverse_probability_weight": format(
                    inverse_probability, ".17g"
                ),
            }
        )

    person_rows: list[dict[str, str]] = []
    for person in selected_people:
        sample_household_id, geography_row, probability = household_lookup[
            person["HOGAR_REF_ID"]
        ]
        try:
            sex = int(person["P02"])
            age = int(person["P03"])
        except ValueError as exc:
            raise TargetYearSamplingError(
                f"invalid_person_domain:{person['PERSONA_REF_ID']}"
            ) from exc
        if sex not in (1, 2) or not (0 <= age <= 120):
            raise TargetYearSamplingError(
                f"invalid_person_domain:{person['PERSONA_REF_ID']}"
            )
        person_rows.append(
            {
                "sample_person_id": _stable_id("person", person["PERSONA_REF_ID"]),
                "sample_household_id": sample_household_id,
                "department_2010_id": geography_row["department_2010_id"],
                "radio_2010_id": geography_row["radio_2010_id"],
                "sex_code": str(sex),
                "age_years": str(age),
                "selection_probability": format(probability, ".17g"),
                "design_inverse_probability_weight": format(
                    1 / probability, ".17g"
                ),
            }
        )

    household_rows.sort(key=lambda row: row["sample_household_id"])
    person_rows.sort(key=lambda row: row["sample_person_id"])
    selected_person_counts = Counter(
        row["department_2010_id"] for row in person_rows
    )
    selected_household_counts = Counter(
        row["department_2010_id"] for row in household_rows
    )
    total_target = sum(target_person_mass.values())
    total_selected_people = len(person_rows)

    department_qa = {}
    for department in sorted(donor_departments):
        target_share = target_person_mass[department] / total_target
        realized_people = selected_person_counts[department]
        realized_share = (
            realized_people / total_selected_people if total_selected_people else 0.0
        )
        department_qa[department] = {
            "donor_person_mass": donor_person_mass[department],
            "target_person_mass": target_person_mass[department],
            "selection_probability": probabilities[department],
            "expected_selected_person_mass": fraction
            * target_person_mass[department],
            "target_person_share": target_share,
            "selected_households": selected_household_counts[department],
            "selected_persons": realized_people,
            "realized_person_share": realized_share,
            "realized_minus_target_person_share": realized_share - target_share,
        }

    source_meta = {
        filename: {
            "sha256": _sha256(source / filename),
            "size_bytes": (source / filename).stat().st_size,
        }
        for filename in SOURCE_FILES
    }
    identity = {
        "contract": CONTRACT,
        "algorithm": ALGORITHM,
        "frame_vintage": 2010,
        "target_year": target_year,
        "fraction": fraction,
        "seed": seed,
        "target_source_id": target_source_id,
        "target_source_sha256": target_meta["sha256"],
        "source_files": source_meta,
        "geography_sha256": _sha256(geo_path),
    }
    identity_hash = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    release_id = f"census-target-{target_year}-{identity_hash[:16]}"
    destination = output_root / release_id
    if destination.exists():
        raise TargetYearSamplingError(f"immutable_release_exists:{destination}")

    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{release_id}.", dir=output_root))
    try:
        _write_csv(staging / "households.csv", HOUSEHOLD_FIELDS, household_rows)
        _write_csv(staging / "persons.csv", PERSON_FIELDS, person_rows)
        qa = {
            "selection_unit": "household",
            "target_mass_unit": "person",
            "frame_vintage": 2010,
            "sampling_target_year": target_year,
            "selected_counts": {
                "households": len(household_rows),
                "persons": len(person_rows),
            },
            "complete_household_membership": (
                len(person_rows)
                == sum(len(people_by_household[household_id]) for household_id in selected_ids)
            ),
            "departments": department_qa,
        }
        (staging / "qa.json").write_text(_canonical_json(qa), encoding="utf-8")
        artifacts = {}
        for filename in ("households.csv", "persons.csv", "qa.json"):
            path = staging / filename
            artifacts[filename] = {
                "sha256": _sha256(path),
                "size_bytes": path.stat().st_size,
            }
        manifest = {
            "contract": CONTRACT,
            "release_id": release_id,
            "frame": {
                "vintage": 2010,
                "source_identity": "Argentina CPV-2010 local authorized copy",
                "tables": source_meta,
                "geography_crosswalk": {
                    "sha256": _sha256(geo_path),
                    "size_bytes": geo_path.stat().st_size,
                },
            },
            "target_population_parent": {
                "source_id": target_source_id,
                "target_year": target_year,
                **target_meta,
            },
            "selection": {
                "algorithm": ALGORITHM,
                "unit": "household",
                "target_mass_unit": "person",
                "fraction": fraction,
                "seed": seed,
                "formula": "p[d,y] = fraction * target_person_mass[d,y] / donor_person_mass[d]",
                "common_score_across_target_years": True,
                "probability_bound_policy": "fail_on_overflow",
            },
            "weight_semantics": {
                "selection_probability": "household inclusion probability under this target-year design",
                "design_inverse_probability_weight": "1 / selection_probability; donor-frame design diagnostic only",
                "analysis_weight": None,
                "generic_sample_weight": None,
            },
            "artifacts": artifacts,
            "qa": qa,
            "scientific_assumptions": [
                "Selected records remain CPV-2010 donor households and persons.",
                "Only relative department person mass is intentionally updated.",
                "Within-department joint distributions remain inherited from CPV-2010.",
                "No contemporary official micro-population is reconstructed.",
            ],
        }
        (staging / "manifest.json").write_text(
            _canonical_json(manifest), encoding="utf-8"
        )
        staging.replace(destination)
        return destination
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
