"""Vintage-neutral governed Census donor-frame contract."""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

FRAME_CONTRACT = "research.census-frame/v1"
REQUIRED_ARTIFACTS = (
    "frame_households.parquet",
    "donor_person_mass.parquet",
    "payload/vivienda.parquet",
    "payload/hogar.parquet",
    "payload/persona.parquet",
)


class CensusFrameError(ValueError):
    """Raised when a prepared Census donor frame is invalid."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _load_manifest(root: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CensusFrameError("frame_manifest_missing_or_invalid") from exc
    if not isinstance(manifest, dict):
        raise CensusFrameError("frame_manifest_missing_or_invalid")
    if manifest.get("contract") != FRAME_CONTRACT:
        raise CensusFrameError("unexpected_frame_contract")
    if manifest.get("census_vintage") not in {2010, 2022}:
        raise CensusFrameError("unsupported_census_vintage")
    release_id = manifest.get("frame_release_id")
    if not isinstance(release_id, str) or not release_id:
        raise CensusFrameError("frame_release_id_missing")
    return manifest


def _require_columns(schema_names: Iterable[str], required: set[str], table: str) -> None:
    missing = sorted(required - set(schema_names))
    if missing:
        raise CensusFrameError(f"{table}:missing_required_columns:{','.join(missing)}")


def _artifact_record(manifest: dict[str, Any], name: str) -> dict[str, Any]:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise CensusFrameError("frame_artifacts_missing")
    record = artifacts.get(name)
    if not isinstance(record, dict):
        raise CensusFrameError(f"frame_artifact_manifest_missing:{name}")
    return record


def _iter_parquet_dicts(path: Path, columns: list[str], batch_size: int = 65536):
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise CensusFrameError("pyarrow_required_for_frame_validation") from exc
    parquet = pq.ParquetFile(path)
    _require_columns(parquet.schema_arrow.names, set(columns), path.name)
    for batch in parquet.iter_batches(batch_size=batch_size, columns=columns):
        values = batch.to_pydict()
        for idx in range(batch.num_rows):
            yield {name: values[name][idx] for name in columns}


def validate_frame(
    root: Path, *, verify_hashes: bool = True, deep: bool = True
) -> dict[str, Any]:
    """Validate one ``research.census-frame/v1`` directory.

    ``deep=False`` checks the immutable custody contract before repeated sample
    runs. ``deep=True`` additionally streams identities through temporary
    SQLite so a national frame can be checked without holding all Census IDs in
    the Python heap.
    """
    import shutil
    import sqlite3
    import tempfile

    root = Path(root).expanduser().resolve()
    manifest = _load_manifest(root)
    for name in REQUIRED_ARTIFACTS:
        path = root / name
        if not path.is_file():
            raise CensusFrameError(f"frame_artifact_missing:{name}")
        record = _artifact_record(manifest, name)
        if verify_hashes and record.get("sha256") != sha256_file(path):
            raise CensusFrameError(f"frame_artifact_hash_mismatch:{name}")

    donor_mass: dict[str, int] = {}
    for row in _iter_parquet_dicts(
        root / "donor_person_mass.parquet", ["department_id", "donor_person_mass"]
    ):
        department = str(row["department_id"] or "")
        if not department or department in donor_mass:
            raise CensusFrameError(f"duplicate_or_empty_donor_department:{department}")
        try:
            mass = int(row["donor_person_mass"])
        except (TypeError, ValueError) as exc:
            raise CensusFrameError(f"invalid_donor_person_mass:{department}") from exc
        if mass <= 0:
            raise CensusFrameError(f"nonpositive_donor_person_mass:{department}")
        donor_mass[department] = mass

    if not deep:
        return {
            "contract": FRAME_CONTRACT,
            "status": "custody-valid",
            "frame_release_id": manifest["frame_release_id"],
            "census_vintage": manifest["census_vintage"],
            "counts": manifest.get("counts") or {},
            "departments": sorted(donor_mass),
            "donor_person_mass": donor_mass,
            "manifest": manifest,
        }

    work = Path(tempfile.mkdtemp(prefix="census-frame-validate-"))
    db = work / "frame.sqlite"
    observed_counts: dict[str, int] = {}
    try:
        conn = sqlite3.connect(db)
        try:
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA temp_store=FILE")
            conn.execute(
                "CREATE TABLE frame_hh (hh TEXT PRIMARY KEY, viv TEXT NOT NULL, dept TEXT NOT NULL, radio TEXT NOT NULL, n INTEGER NOT NULL)"
            )
            conn.execute("CREATE TABLE vivienda (viv TEXT PRIMARY KEY)")
            conn.execute("CREATE TABLE hogar (hh TEXT PRIMARY KEY, viv TEXT NOT NULL)")
            conn.execute("CREATE TABLE persona (person TEXT PRIMARY KEY, hh TEXT NOT NULL)")
            conn.execute("CREATE TABLE hh_counts (hh TEXT PRIMARY KEY, n INTEGER NOT NULL)")

            donor_from_households: Counter[str] = Counter()
            hh_count = 0
            try:
                for row in _iter_parquet_dicts(
                    root / "frame_households.parquet",
                    [
                        "frame_household_id",
                        "frame_dwelling_id",
                        "department_id",
                        "radio_id",
                        "household_person_count",
                    ],
                ):
                    hh = str(row["frame_household_id"] or "")
                    viv = str(row["frame_dwelling_id"] or "")
                    dept = str(row["department_id"] or "")
                    radio = str(row["radio_id"] or "")
                    try:
                        n = int(row["household_person_count"])
                    except (TypeError, ValueError) as exc:
                        raise CensusFrameError(f"invalid_household_person_count:{hh}") from exc
                    if not hh or not viv or not dept or not radio:
                        raise CensusFrameError(f"empty_frame_relation:{hh}")
                    if n <= 0:
                        raise CensusFrameError(f"nonpositive_household_person_count:{hh}")
                    conn.execute(
                        "INSERT INTO frame_hh(hh,viv,dept,radio,n) VALUES (?,?,?,?,?)",
                        (hh, viv, dept, radio, n),
                    )
                    donor_from_households[dept] += n
                    hh_count += 1
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise CensusFrameError("duplicate_frame_household_id") from exc
            if donor_mass != dict(donor_from_households):
                raise CensusFrameError("donor_person_mass_mismatch")

            vivienda_count = 0
            try:
                for row in _iter_parquet_dicts(
                    root / "payload/vivienda.parquet", ["frame_dwelling_id"]
                ):
                    value = str(row["frame_dwelling_id"] or "")
                    if not value:
                        raise CensusFrameError("vivienda:empty_frame_dwelling_id")
                    conn.execute("INSERT INTO vivienda(viv) VALUES (?)", (value,))
                    vivienda_count += 1
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise CensusFrameError("vivienda:duplicate_frame_dwelling_id") from exc
            missing_dwellings = conn.execute(
                "SELECT COUNT(*) FROM frame_hh f LEFT JOIN vivienda v ON v.viv=f.viv WHERE v.viv IS NULL"
            ).fetchone()[0]
            if missing_dwellings:
                raise CensusFrameError("frame_household_missing_payload_dwelling")

            payload_hh_count = 0
            try:
                for row in _iter_parquet_dicts(
                    root / "payload/hogar.parquet",
                    ["frame_household_id", "frame_dwelling_id"],
                ):
                    hh = str(row["frame_household_id"] or "")
                    viv = str(row["frame_dwelling_id"] or "")
                    if not hh or not viv:
                        raise CensusFrameError("hogar:empty_frame_identity")
                    conn.execute("INSERT INTO hogar(hh,viv) VALUES (?,?)", (hh, viv))
                    payload_hh_count += 1
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise CensusFrameError("hogar:duplicate_frame_household_id") from exc
            orphan_hogar_viv = conn.execute(
                "SELECT COUNT(*) FROM hogar h LEFT JOIN vivienda v ON v.viv=h.viv WHERE v.viv IS NULL"
            ).fetchone()[0]
            hh_missing_payload = conn.execute(
                "SELECT COUNT(*) FROM frame_hh f LEFT JOIN hogar h ON h.hh=f.hh WHERE h.hh IS NULL"
            ).fetchone()[0]
            hh_extra_payload = conn.execute(
                "SELECT COUNT(*) FROM hogar h LEFT JOIN frame_hh f ON f.hh=h.hh WHERE f.hh IS NULL"
            ).fetchone()[0]
            if orphan_hogar_viv:
                raise CensusFrameError("hogar:orphan_frame_dwelling_id")
            if hh_missing_payload or hh_extra_payload:
                raise CensusFrameError("frame_household_index_payload_mismatch")

            person_count = 0
            try:
                for row in _iter_parquet_dicts(
                    root / "payload/persona.parquet",
                    ["frame_person_id", "frame_household_id"],
                ):
                    person = str(row["frame_person_id"] or "")
                    hh = str(row["frame_household_id"] or "")
                    if not person or not hh:
                        raise CensusFrameError("persona:empty_frame_identity")
                    conn.execute("INSERT INTO persona(person,hh) VALUES (?,?)", (person, hh))
                    conn.execute(
                        "INSERT INTO hh_counts(hh,n) VALUES (?,1) ON CONFLICT(hh) DO UPDATE SET n=n+1",
                        (hh,),
                    )
                    person_count += 1
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise CensusFrameError("persona:duplicate_frame_person_id") from exc
            orphan_people = conn.execute(
                "SELECT COUNT(*) FROM persona p LEFT JOIN frame_hh f ON f.hh=p.hh WHERE f.hh IS NULL"
            ).fetchone()[0]
            size_mismatch = conn.execute(
                "SELECT COUNT(*) FROM frame_hh f LEFT JOIN hh_counts c ON c.hh=f.hh WHERE COALESCE(c.n,0) != f.n"
            ).fetchone()[0]
            if orphan_people:
                raise CensusFrameError("persona:orphan_frame_household_id")
            if size_mismatch:
                raise CensusFrameError("household_person_count_payload_mismatch")

            observed_counts = {
                "dwellings": vivienda_count,
                "households": hh_count,
                "persons": person_count,
                "departments": len(donor_mass),
            }
            manifest_counts = manifest.get("counts") or {}
            for key, observed in observed_counts.items():
                expected = manifest_counts.get(key)
                if expected is not None and expected != observed:
                    raise CensusFrameError(f"frame_manifest_count_mismatch:{key}")
        finally:
            conn.close()
    finally:
        shutil.rmtree(work, ignore_errors=True)

    return {
        "contract": FRAME_CONTRACT,
        "status": "valid",
        "frame_release_id": manifest["frame_release_id"],
        "census_vintage": manifest["census_vintage"],
        "counts": observed_counts,
        "departments": sorted(donor_mass),
        "donor_person_mass": donor_mass,
        "manifest": manifest,
    }
