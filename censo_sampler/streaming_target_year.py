"""Streaming/SQLite backend for governed CPV-2010 target-year sampling.

This module is an execution-scale replacement for the in-memory implementation
in ``target_year.py``. It intentionally reuses the same contracts, score,
identity function, probability formula and manifest schema. On a bounded donor
fixture, both backends must produce byte-identical releases.

The complete CPV-2010 dataset is not exercised in CI. This backend exists so a
future authorized local run does not require loading every Census row into the
Python heap.
"""
from __future__ import annotations

import csv
import json
import math
import shutil
import sqlite3
import tempfile
from collections import Counter
from pathlib import Path
from typing import Iterator

from .target_year import (
    ALGORITHM,
    CONTRACT,
    HOUSEHOLD_FIELDS,
    PERSON_FIELDS,
    SOURCE_FILES,
    TARGET_YEARS,
    TargetYearSamplingError,
    _canonical_json,
    _dialect,
    _score,
    _sha256,
    _stable_id,
    _target_mass,
    _write_csv,
)


def _iter_rows(
    path: Path,
    required: tuple[str, ...],
    table: str,
) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, dialect=_dialect(path))
        fields = set(reader.fieldnames or [])
        missing = sorted(set(required) - fields)
        if missing:
            raise TargetYearSamplingError(
                f"{table}:missing_required_columns:{','.join(missing)}"
            )
        for row in reader:
            yield {key: (value or "").strip() for key, value in row.items()}


def _insert_unique(
    conn: sqlite3.Connection,
    statement: str,
    rows: Iterator[tuple[str, ...]],
    *,
    table: str,
    key: str,
    batch_size: int = 50000,
) -> int:
    count = 0
    batch: list[tuple[str, ...]] = []
    for values in rows:
        if not values[0]:
            raise TargetYearSamplingError(f"{table}:duplicate_or_empty_{key}:['']")
        batch.append(values)
        if len(batch) >= batch_size:
            try:
                conn.executemany(statement, batch)
            except sqlite3.IntegrityError as exc:
                raise TargetYearSamplingError(
                    f"{table}:duplicate_or_empty_{key}"
                ) from exc
            count += len(batch)
            batch.clear()
    if batch:
        try:
            conn.executemany(statement, batch)
        except sqlite3.IntegrityError as exc:
            raise TargetYearSamplingError(
                f"{table}:duplicate_or_empty_{key}"
            ) from exc
        count += len(batch)
    return count


def build_target_year_release_streaming(
    databasepath: Path,
    output_root: Path,
    *,
    target_population_path: Path,
    target_source_id: str,
    target_year: int,
    fraction: float = 0.01,
    seed: int = 20260831,
    geography_path: Path | None = None,
    max_households: int | None = None,
) -> Path:
    """Build one target-year release without materializing the donor frame in RAM."""
    if target_year not in TARGET_YEARS:
        raise TargetYearSamplingError(
            f"target_year_must_be_one_of:{sorted(TARGET_YEARS)}"
        )
    if not (0 < fraction <= 1):
        raise TargetYearSamplingError("fraction_must_be_in_(0,1]")
    if max_households is not None and max_households < 1:
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

    target_person_mass, target_meta = _target_mass(target_path, target_year)
    source_meta = {
        filename: {
            "sha256": _sha256(source / filename),
            "size_bytes": (source / filename).stat().st_size,
        }
        for filename in SOURCE_FILES
    }
    geography_sha256 = _sha256(geo_path)
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
        "geography_sha256": geography_sha256,
    }
    import hashlib

    identity_hash = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    release_id = f"census-target-{target_year}-{identity_hash[:16]}"
    destination = output_root / release_id
    if destination.exists():
        raise TargetYearSamplingError(f"immutable_release_exists:{destination}")

    output_root.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=f".{release_id}.work.", dir=output_root))
    db_path = work / "donor.sqlite"
    staging = work / "release"
    staging.mkdir()
    try:
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA temp_store=FILE")
            conn.execute(
                "CREATE TABLE geo (radio TEXT PRIMARY KEY, radio_2010 TEXT NOT NULL, dept TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE vivienda (viv TEXT PRIMARY KEY, radio TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE hogar (hh TEXT PRIMARY KEY, viv TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE persona (person TEXT PRIMARY KEY, hh TEXT NOT NULL, sex TEXT NOT NULL, age TEXT NOT NULL)"
            )

            geography_count = _insert_unique(
                conn,
                "INSERT INTO geo(radio,radio_2010,dept) VALUES (?,?,?)",
                (
                    (
                        row["RADIO_REF_ID"],
                        row["radio_2010_id"],
                        row["department_2010_id"],
                    )
                    for row in _iter_rows(
                        geo_path,
                        ("RADIO_REF_ID", "radio_2010_id", "department_2010_id"),
                        "GEOGRAPHY",
                    )
                ),
                table="GEOGRAPHY",
                key="RADIO_REF_ID",
            )
            vivienda_count = _insert_unique(
                conn,
                "INSERT INTO vivienda(viv,radio) VALUES (?,?)",
                (
                    (row["VIVIENDA_REF_ID"], row["RADIO_REF_ID"])
                    for row in _iter_rows(
                        source / "VIVIENDA.csv",
                        ("VIVIENDA_REF_ID", "RADIO_REF_ID"),
                        "VIVIENDA",
                    )
                ),
                table="VIVIENDA",
                key="VIVIENDA_REF_ID",
            )
            household_count = _insert_unique(
                conn,
                "INSERT INTO hogar(hh,viv) VALUES (?,?)",
                (
                    (row["HOGAR_REF_ID"], row["VIVIENDA_REF_ID"])
                    for row in _iter_rows(
                        source / "HOGAR.csv",
                        ("HOGAR_REF_ID", "VIVIENDA_REF_ID"),
                        "HOGAR",
                    )
                ),
                table="HOGAR",
                key="HOGAR_REF_ID",
            )
            if max_households is not None and household_count > max_households:
                raise TargetYearSamplingError(
                    f"bounded_release_limit_exceeded:{household_count}>{max_households}"
                )
            person_count = _insert_unique(
                conn,
                "INSERT INTO persona(person,hh,sex,age) VALUES (?,?,?,?)",
                (
                    (
                        row["PERSONA_REF_ID"],
                        row["HOGAR_REF_ID"],
                        row["P02"],
                        row["P03"],
                    )
                    for row in _iter_rows(
                        source / "PERSONA.csv",
                        ("PERSONA_REF_ID", "HOGAR_REF_ID", "P02", "P03"),
                        "PERSONA",
                    )
                ),
                table="PERSONA",
                key="PERSONA_REF_ID",
            )
            conn.commit()

            missing_viv = conn.execute(
                "SELECT COUNT(*) FROM hogar h LEFT JOIN vivienda v ON v.viv=h.viv WHERE v.viv IS NULL"
            ).fetchone()[0]
            orphan_people = conn.execute(
                "SELECT COUNT(*) FROM persona p LEFT JOIN hogar h ON h.hh=p.hh WHERE h.hh IS NULL"
            ).fetchone()[0]
            unresolved_radios = conn.execute(
                "SELECT COUNT(*) FROM vivienda v LEFT JOIN geo g ON g.radio=v.radio WHERE g.radio IS NULL"
            ).fetchone()[0]
            if missing_viv or orphan_people or unresolved_radios:
                raise TargetYearSamplingError(
                    "donor_relational_failure:"
                    f"missing_viviendas={missing_viv}:"
                    f"orphan_person_households={orphan_people}:"
                    f"missing_radios={unresolved_radios}"
                )

            conn.execute(
                "CREATE TABLE hh_geo AS "
                "SELECT h.hh AS hh, g.radio_2010 AS radio_2010, g.dept AS dept "
                "FROM hogar h JOIN vivienda v ON v.viv=h.viv JOIN geo g ON g.radio=v.radio"
            )
            conn.execute("CREATE UNIQUE INDEX hh_geo_pk ON hh_geo(hh)")
            conn.execute(
                "CREATE TABLE donor_mass AS "
                "SELECT g.dept AS dept, COUNT(*) AS n "
                "FROM persona p JOIN hh_geo g ON g.hh=p.hh GROUP BY g.dept"
            )
            conn.commit()

            donor_person_mass = {
                str(dept): int(mass)
                for dept, mass in conn.execute("SELECT dept,n FROM donor_mass")
            }
            if sum(donor_person_mass.values()) != person_count:
                raise TargetYearSamplingError(
                    "donor_person_mass_does_not_sum_to_person_count"
                )
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
                probability = (
                    fraction
                    * target_person_mass[department]
                    / donor_person_mass[department]
                )
                if not math.isfinite(probability) or probability <= 0:
                    raise TargetYearSamplingError(
                        f"invalid_selection_probability:{department}:{probability}"
                    )
                if probability > 1:
                    raise TargetYearSamplingError(
                        f"selection_probability_overflow:{department}:{probability:.12g}"
                    )
                probabilities[department] = probability

            conn.execute(
                "CREATE TABLE selected_hh (hh TEXT PRIMARY KEY, dept TEXT NOT NULL, radio_2010 TEXT NOT NULL, probability REAL NOT NULL)"
            )
            selected_household_counts: Counter[str] = Counter()
            for household_id, radio_2010, department in conn.execute(
                "SELECT hh,radio_2010,dept FROM hh_geo ORDER BY hh"
            ):
                probability = probabilities[str(department)]
                if _score(seed, str(household_id), str(department)) < probability:
                    conn.execute(
                        "INSERT INTO selected_hh(hh,dept,radio_2010,probability) VALUES (?,?,?,?)",
                        (
                            str(household_id),
                            str(department),
                            str(radio_2010),
                            probability,
                        ),
                    )
                    selected_household_counts[str(department)] += 1
            conn.commit()
            selected_households = sum(selected_household_counts.values())
            if not selected_households:
                raise TargetYearSamplingError("deterministic_selection_empty")

            household_rows: list[dict[str, str]] = []
            for household_id, department, probability in conn.execute(
                "SELECT hh,dept,probability FROM selected_hh ORDER BY hh"
            ):
                household_rows.append(
                    {
                        "sample_household_id": _stable_id(
                            "household", str(household_id)
                        ),
                        "department_2010_id": str(department),
                        "selection_probability": format(float(probability), ".17g"),
                        "design_inverse_probability_weight": format(
                            1 / float(probability), ".17g"
                        ),
                    }
                )

            person_rows: list[dict[str, str]] = []
            selected_person_counts: Counter[str] = Counter()
            for (
                person_id,
                household_id,
                sex_raw,
                age_raw,
                department,
                radio_2010,
                probability,
            ) in conn.execute(
                "SELECT p.person,p.hh,p.sex,p.age,s.dept,s.radio_2010,s.probability "
                "FROM persona p JOIN selected_hh s ON s.hh=p.hh ORDER BY p.person"
            ):
                try:
                    sex = int(sex_raw)
                    age = int(age_raw)
                except ValueError as exc:
                    raise TargetYearSamplingError(
                        f"invalid_person_domain:{person_id}"
                    ) from exc
                if sex not in (1, 2) or not (0 <= age <= 120):
                    raise TargetYearSamplingError(
                        f"invalid_person_domain:{person_id}"
                    )
                selected_person_counts[str(department)] += 1
                person_rows.append(
                    {
                        "sample_person_id": _stable_id("person", str(person_id)),
                        "sample_household_id": _stable_id(
                            "household", str(household_id)
                        ),
                        "department_2010_id": str(department),
                        "radio_2010_id": str(radio_2010),
                        "sex_code": str(sex),
                        "age_years": str(age),
                        "selection_probability": format(float(probability), ".17g"),
                        "design_inverse_probability_weight": format(
                            1 / float(probability), ".17g"
                        ),
                    }
                )
            if not person_rows:
                raise TargetYearSamplingError("selected_households_have_no_people")

            total_target = sum(target_person_mass.values())
            total_selected_people = len(person_rows)
            department_qa = {}
            for department in sorted(donor_departments):
                target_share = target_person_mass[department] / total_target
                realized_people = selected_person_counts[department]
                realized_share = realized_people / total_selected_people
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
        finally:
            conn.close()

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
            "complete_household_membership": True,
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
                    "sha256": geography_sha256,
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
        db_path.unlink(missing_ok=True)
        work.rmdir()
        return destination
    except Exception:
        shutil.rmtree(work, ignore_errors=True)
        raise
