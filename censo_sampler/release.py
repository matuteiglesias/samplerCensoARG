"""Deterministic, bounded ``research.census-sample/v1`` release producer.

This module intentionally uses the standard library for input validation and
canonical CSV output.  It never copies source microdata into the release.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import platform
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

CONTRACT = "research.census-sample/v1"
ALGORITHM = "sha256-household-bernoulli/v1"
SOURCE_FILES = ("VIVIENDA.csv", "HOGAR.csv", "PERSONA.csv")
PERSON_FIELDS = ["sample_person_id", "sample_household_id", "sex_code", "age_years", "radio_2010_id", "sample_weight"]
HOUSEHOLD_FIELDS = ["sample_household_id", "department_2010_id", "region_id", "sample_weight"]
REGIONS = {"gran_buenos_aires", "cuyo", "noreste", "noroeste", "pampeana", "patagonia"}


class ReleaseError(ValueError):
    """A hard publication failure."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _dialect(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        sample = stream.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;")
    except csv.Error:
        return csv.excel


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return [{key: (value or "").strip() for key, value in row.items()} for row in csv.DictReader(stream, dialect=_dialect(path))]


def _require(rows, columns, table):
    present = set(rows[0]) if rows else set()
    missing = sorted(set(columns) - present)
    if missing:
        raise ReleaseError(f"{table}: missing required columns: {', '.join(missing)}")


def _unique(rows, key, table):
    values = [row[key] for row in rows]
    bad = sorted(value for value, count in Counter(values).items() if not value or count > 1)
    if bad:
        raise ReleaseError(f"{table}: duplicate or empty {key}: {bad[:5]}")


def _score(seed: int, household_id: str, stratum: str) -> float:
    payload = f"{seed}\x1f{household_id}\x1f{stratum}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest(), "big") / 2**256


def _stable_id(namespace, unit, source_id):
    return f"cpv2010:{namespace}:{unit}:{source_id}"


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _write_csv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _summary(values):
    values = sorted(values)
    if not values:
        return {"min": None, "max": None, "mean": None}
    return {"min": values[0], "max": values[-1], "mean": sum(values) / len(values)}


def build_release(databasepath, output_root, *, fraction, seed, analysis_period, name="ARG",
                  weight_policy="cpv2010_frame_inverse_probability", departments=None,
                  geography_path=None, handoff_dir=None, max_households=100000):
    """Validate local CPV-like CSVs and atomically create one immutable release."""
    if not (0 < fraction <= 1):
        raise ReleaseError("fraction must be in (0, 1]")
    if max_households < 1:
        raise ReleaseError("max_households must be positive")
    if weight_policy not in {"cpv2010_frame_inverse_probability", "legacy_department_projection_candidate"}:
        raise ReleaseError(f"unsupported weight policy: {weight_policy}")
    source = Path(databasepath).expanduser().resolve()
    missing = [filename for filename in SOURCE_FILES if not (source / filename).is_file()]
    if missing:
        raise ReleaseError(f"CPV-2010 source directory {source} requires: {', '.join(missing)}")
    output_root = Path(output_root).expanduser().resolve()
    if output_root == source or source in output_root.parents:
        raise ReleaseError("unsafe output path: release cannot be inside the source directory")

    source_meta = {filename: {"sha256": sha256_file(source / filename), "size_bytes": (source / filename).stat().st_size} for filename in SOURCE_FILES}
    vivienda, hogares, personas = (_rows(source / filename) for filename in SOURCE_FILES)
    _require(vivienda, ["VIVIENDA_REF_ID", "RADIO_REF_ID"], "VIVIENDA")
    _require(hogares, ["HOGAR_REF_ID", "VIVIENDA_REF_ID"], "HOGAR")
    _require(personas, ["PERSONA_REF_ID", "HOGAR_REF_ID", "P02", "P03"], "PERSONA")
    _unique(vivienda, "VIVIENDA_REF_ID", "VIVIENDA")
    _unique(hogares, "HOGAR_REF_ID", "HOGAR")
    _unique(personas, "PERSONA_REF_ID", "PERSONA")
    viv_by_id = {row["VIVIENDA_REF_ID"]: row for row in vivienda}
    hh_by_id = {row["HOGAR_REF_ID"]: row for row in hogares}
    missing_viv = sorted({row["VIVIENDA_REF_ID"] for row in hogares} - set(viv_by_id))
    orphan_people = sorted({row["HOGAR_REF_ID"] for row in personas} - set(hh_by_id))
    if missing_viv or orphan_people:
        raise ReleaseError(f"source relational failure: missing viviendas={missing_viv[:5]}, orphan persons={orphan_people[:5]}")

    geo_path = Path(geography_path).resolve() if geography_path else source / "GEOGRAPHY.csv"
    if not geo_path.is_file():
        raise ReleaseError("missing deterministic geography crosswalk; provide --geography with RADIO_REF_ID,radio_2010_id,department_2010_id,region_id")
    geo_rows = _rows(geo_path)
    _require(geo_rows, ["RADIO_REF_ID", "radio_2010_id", "department_2010_id", "region_id"], "GEOGRAPHY")
    _unique(geo_rows, "RADIO_REF_ID", "GEOGRAPHY")
    geo = {row["RADIO_REF_ID"]: row for row in geo_rows}
    unresolved = sorted({row["RADIO_REF_ID"] for row in vivienda} - set(geo))
    invalid_regions = sorted({row["region_id"] for row in geo_rows} - REGIONS)
    if unresolved or invalid_regions:
        raise ReleaseError(f"geography failure: missing radios={unresolved[:5]}, invalid regions={invalid_regions}")

    dept_filter = set(map(str, departments or []))
    frame = []
    certainty = []
    warnings = []
    for hh in hogares:
        g = geo[viv_by_id[hh["VIVIENDA_REF_ID"]]["RADIO_REF_ID"]]
        dept = g["department_2010_id"]
        if dept_filter and dept not in dept_filter:
            continue
        multiplier = float(g.get("frame_selection_multiplier") or 1)
        probability = min(1.0, fraction * multiplier)
        if probability == 1 and fraction * multiplier > 1:
            certainty.append(dept)
        frame.append((hh, g, probability))
    if not frame:
        raise ReleaseError("geographic selection produced an empty household frame")
    if len(frame) > max_households:
        raise ReleaseError(f"bounded release limit exceeded: frame has {len(frame)} households (limit {max_households})")
    if certainty:
        warnings.append("inclusion probability capped at one for certainty strata")
    if dept_filter:
        warnings.append("bounded department subset; release is not nationwide")
    if weight_policy == "legacy_department_projection_candidate":
        warnings.append("legacy projection factors are historical compatibility only, not a current official estimate")

    config = {"source_vintage": "Argentina CPV-2010", "algorithm": ALGORITHM, "seed": seed, "fraction": fraction, "analysis_period": analysis_period,
              "name": name, "weight_policy": weight_policy, "departments": sorted(dept_filter),
              "geography_contract": "radio-department-six-region/v1"}
    config_hash = hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    namespace = f"2010-{ALGORITHM.rsplit('/', 1)[-1]}-{config_hash[:16]}"
    release_id = f"{CONTRACT.replace('/', '-')}-{config_hash[:20]}"
    selected = [(hh, g, p) for hh, g, p in frame if _score(seed, hh["HOGAR_REF_ID"], g["department_2010_id"]) < p]
    selected_ids = {hh["HOGAR_REF_ID"] for hh, _, _ in selected}
    selected_people = [person for person in personas if person["HOGAR_REF_ID"] in selected_ids]
    if not selected or not selected_people:
        raise ReleaseError("deterministic selection is empty; increase fraction or scope")

    hh_out, hh_lookup = [], {}
    for hh, g, probability in selected:
        factor = float(g.get("projection_factor") or 1)
        base_weight = 1 / probability
        final_weight = base_weight * factor if weight_policy == "legacy_department_projection_candidate" else base_weight
        if not math.isfinite(final_weight) or final_weight <= 0:
            raise ReleaseError("nonfinite or nonpositive final sample weight")
        sample_id = _stable_id(namespace, "household", hh["HOGAR_REF_ID"])
        hh_lookup[hh["HOGAR_REF_ID"]] = (sample_id, g, final_weight)
        hh_out.append({"sample_household_id": sample_id, "department_2010_id": g["department_2010_id"],
                       "region_id": g["region_id"], "sample_weight": format(final_weight, ".12g")})
    person_out = []
    for person in selected_people:
        sample_hh, g, weight = hh_lookup[person["HOGAR_REF_ID"]]
        try:
            sex, age = int(person["P02"]), int(person["P03"])
        except ValueError as exc:
            raise ReleaseError(f"invalid age/sex domain for person {person['PERSONA_REF_ID']}") from exc
        if sex not in (1, 2) or not (0 <= age <= 120):
            raise ReleaseError(f"invalid age/sex domain for person {person['PERSONA_REF_ID']}")
        person_out.append({"sample_person_id": _stable_id(namespace, "person", person["PERSONA_REF_ID"]),
                           "sample_household_id": sample_hh, "sex_code": str(sex), "age_years": str(age),
                           "radio_2010_id": g["radio_2010_id"], "sample_weight": format(weight, ".12g")})
    hh_out.sort(key=lambda row: row["sample_household_id"])
    person_out.sort(key=lambda row: row["sample_person_id"])

    counts_dept = Counter(row["department_2010_id"] for row in hh_out)
    counts_region = Counter(row["region_id"] for row in hh_out)
    frame_dept = Counter(g["department_2010_id"] for _, g, _ in frame)
    probability_dept = defaultdict(list)
    for _, g, probability in frame:
        probability_dept[g["department_2010_id"]].append(probability)
    qa = {"source_counts": {"viviendas": len(vivienda), "households": len(hogares), "persons": len(personas)},
          "selected_counts": {"households": len(hh_out), "persons": len(person_out)},
          "households_by_department": dict(sorted(counts_dept.items())), "households_by_region": dict(sorted(counts_region.items())),
          "certainty_departments": sorted(set(certainty)), "duplicate_source_ids": 0, "orphan_persons": 0,
          "missing_geography": 0, "household_id_unique": len({r['sample_household_id'] for r in hh_out}) == len(hh_out),
          "person_id_unique": len({r['sample_person_id'] for r in person_out}) == len(person_out), "foreign_key_coverage": True,
          "department_sampling": {dept: {"frame_households": frame_dept[dept], "selected_households": counts_dept[dept],
                                         "realized_fraction": counts_dept[dept] / frame_dept[dept],
                                         "requested_probability_range": [min(probability_dept[dept]), max(probability_dept[dept])]}
                                  for dept in sorted(frame_dept)},
          "weight_summary": _summary([float(row["sample_weight"]) for row in hh_out]),
          "age_distribution": dict(sorted(Counter(row["age_years"] for row in person_out).items(), key=lambda x: int(x[0]))),
          "sex_distribution": dict(sorted(Counter(row["sex_code"] for row in person_out).items()))}

    release_dir = output_root / release_id
    if release_dir.exists():
        raise ReleaseError(f"immutable release already exists: {release_dir}")
    output_root.mkdir(parents=True, exist_ok=True)
    staging = output_root / f".{release_id}.tmp"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir()
    _write_csv(staging / "households.csv", HOUSEHOLD_FIELDS, hh_out)
    _write_csv(staging / "persons.csv", PERSON_FIELDS, person_out)
    (staging / "qa.json").write_text(json.dumps(qa, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (staging / "QA.md").write_text(
        f"# Census sample QA\n\n- Release: `{release_id}`\n- Households: {len(hh_out)}\n- Persons: {len(person_out)}\n"
        f"- Departments: {', '.join(sorted(counts_dept))}\n- Regions: {', '.join(sorted(counts_region))}\n"
        "- Limitation: a CPV-2010 frame does not become a current official population estimate when projected to a later period.\n"
        f"- Warnings: {'; '.join(warnings) or 'none'}\n", encoding="utf-8")
    artifact_names = ["households.csv", "persons.csv", "qa.json", "QA.md"]
    artifacts = {filename: {"size_bytes": (staging / filename).stat().st_size, "sha256": sha256_file(staging / filename)} for filename in artifact_names}
    manifest = {"contract": CONTRACT, "release_id": release_id, "created_at": datetime.now(timezone.utc).isoformat(),
                "source": {"identity": "Argentina CPV-2010 local authorized copy", "tables": source_meta,
                           "geography_crosswalk": {"sha256": sha256_file(geo_path), "size_bytes": geo_path.stat().st_size},
                           "required_columns": {"VIVIENDA.csv": ["VIVIENDA_REF_ID", "RADIO_REF_ID"], "HOGAR.csv": ["HOGAR_REF_ID", "VIVIENDA_REF_ID"], "PERSONA.csv": ["PERSONA_REF_ID", "HOGAR_REF_ID", "P02", "P03"]},
                           "rights": "Not redistributed; operator is responsible for lawful local access."},
                "selection": {"algorithm": ALGORITHM, "unit": "household", "seed": seed, "requested_fraction": fraction,
                              "geographic_selection": sorted(dept_filter) or "all crosswalk-supported departments",
                              "requested_analysis_period": analysis_period, "certainty_policy": "cap inclusion probability at one"},
                "sample_id_namespace": namespace, "weights": {"policy": weight_policy, "frame_selection_probability": "min(1, requested_fraction * explicit frame multiplier)",
                              "base_analysis_weight": "1 / frame_selection_probability", "projection_policy": "separate department factor; applied only by legacy policy"},
                "realized": qa["selected_counts"], "qa": qa, "warnings": warnings,
                "limitations": ["CPV-2010 frame is not a current official population estimate", "No income inference or poverty calculation"],
                "producer": {"commit": _git_commit(), "python": sys.version.split()[0], "platform": platform.platform()}, "files": artifacts}
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    checks = {**artifacts, "manifest.json": {"sha256": sha256_file(staging / "manifest.json")}}
    (staging / "CHECKSUMS.sha256").write_text("".join(f"{value['sha256']}  {filename}\n" for filename, value in sorted(checks.items())), encoding="ascii")
    staging.rename(release_dir)
    if handoff_dir:
        handoff = Path(handoff_dir).expanduser().resolve() / release_id
        if handoff.exists():
            raise ReleaseError(f"immutable handoff already exists: {handoff}")
        handoff.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(release_dir, handoff)
    return release_dir


def check_release(path):
    """Checksum and contract validation without loading artifacts into pandas."""
    release = Path(path).resolve()
    manifest_path = release / "manifest.json"
    checksums = release / "CHECKSUMS.sha256"
    if not manifest_path.is_file() or not checksums.is_file():
        raise ReleaseError("release requires manifest.json and CHECKSUMS.sha256")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("contract") != CONTRACT:
        raise ReleaseError(f"unsupported contract: {manifest.get('contract')}")
    for line in checksums.read_text(encoding="ascii").splitlines():
        expected, filename = line.split("  ", 1)
        target = release / filename
        if not target.is_file() or sha256_file(target) != expected:
            raise ReleaseError(f"checksum mismatch: {filename}")
    for filename, fields in (("persons.csv", PERSON_FIELDS), ("households.csv", HOUSEHOLD_FIELDS)):
        with (release / filename).open(encoding="utf-8", newline="") as stream:
            if next(csv.reader(stream), None) != fields:
                raise ReleaseError(f"canonical header mismatch: {filename}")
    return manifest
