"""Create an auditable metadata lock for authorized local CPV-2010 microdata.

The lock never republishes Census microdata. It records exact source hashes,
validates the PERSONA -> HOGAR -> VIVIENDA -> RADIO -> department graph, and
emits donor person mass by 2010 department. Temporary SQLite keeps the full
relational preflight off the Python heap so the same implementation can later
run against the authorized complete donor frame.
"""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

CONTRACT = "research.cpv2010-donor-frame-lock/v1"
SOURCE_FILES = ("VIVIENDA.csv", "HOGAR.csv", "PERSONA.csv")


class DonorFrameError(ValueError):
    """Raised when the CPV-2010 donor frame cannot be locked safely."""


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


def _iter_rows(path: Path, required: tuple[str, ...], table: str) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, dialect=_dialect(path))
        fields = set(reader.fieldnames or [])
        missing = sorted(set(required) - fields)
        if missing:
            raise DonorFrameError(f"{table}:missing_required_columns:{','.join(missing)}")
        for row in reader:
            yield {key: (value or "").strip() for key, value in row.items()}


def _source_meta(path: Path) -> dict[str, object]:
    return {
        "filename": path.name,
        "sha256": _sha256(path),
        "size_bytes": path.stat().st_size,
    }


def _execute_unique_rows(
    conn: sqlite3.Connection,
    statement: str,
    rows: Iterator[tuple[str, ...]],
    *,
    duplicate_reason: str,
    batch_size: int = 50000,
) -> int:
    count = 0
    batch: list[tuple[str, ...]] = []
    for values in rows:
        if not all(values):
            raise DonorFrameError(f"empty_identity_or_relation:{duplicate_reason}")
        batch.append(values)
        if len(batch) >= batch_size:
            try:
                conn.executemany(statement, batch)
            except sqlite3.IntegrityError as exc:
                raise DonorFrameError(duplicate_reason) from exc
            count += len(batch)
            batch.clear()
    if batch:
        try:
            conn.executemany(statement, batch)
        except sqlite3.IntegrityError as exc:
            raise DonorFrameError(duplicate_reason) from exc
        count += len(batch)
    return count


def _write_mass(path: Path, masses: list[tuple[str, int]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["department_2010_id", "donor_person_mass"])
        writer.writerows(masses)


def build_donor_frame_lock(
    databasepath: Path,
    output_root: Path,
    *,
    geography_path: Path | None = None,
) -> Path:
    """Validate and lock one exact local CPV-2010 donor frame without copying it."""
    source = Path(databasepath).expanduser().resolve()
    output_root = Path(output_root).expanduser().resolve()
    missing = [name for name in SOURCE_FILES if not (source / name).is_file()]
    if missing:
        raise DonorFrameError("missing_cpv2010_source_files:" + ",".join(missing))
    geography = (
        Path(geography_path).expanduser().resolve()
        if geography_path is not None
        else source / "GEOGRAPHY.csv"
    )
    if not geography.is_file():
        raise DonorFrameError("missing_geography_crosswalk")
    if output_root == source or source in output_root.parents:
        raise DonorFrameError("unsafe_output_path_inside_source")

    source_meta = {name: _source_meta(source / name) for name in SOURCE_FILES}
    geography_meta = _source_meta(geography)
    identity = {
        "contract": CONTRACT,
        "frame_vintage": 2010,
        "source_files": {
            name: record["sha256"] for name, record in source_meta.items()
        },
        "geography_sha256": geography_meta["sha256"],
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    release_id = f"cpv2010-donor-frame-lock-{digest[:16]}"
    destination = output_root / release_id
    if destination.exists():
        manifest_path = destination / "manifest.json"
        if manifest_path.is_file():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest.get("release_id") == release_id:
                return destination
        raise DonorFrameError(f"immutable_release_exists:{destination}")

    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{release_id}.", dir=output_root))
    db_path = staging / ".donor-preflight.sqlite"
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
            conn.execute("CREATE TABLE person_id (person TEXT PRIMARY KEY)")
            conn.execute(
                "CREATE TABLE hh_counts (hh TEXT PRIMARY KEY, n INTEGER NOT NULL)"
            )

            geography_rows = _iter_rows(
                geography,
                ("RADIO_REF_ID", "radio_2010_id", "department_2010_id"),
                "GEOGRAPHY",
            )
            geography_count = _execute_unique_rows(
                conn,
                "INSERT INTO geo(radio, radio_2010, dept) VALUES (?, ?, ?)",
                (
                    (
                        row["RADIO_REF_ID"],
                        row["radio_2010_id"],
                        row["department_2010_id"],
                    )
                    for row in geography_rows
                ),
                duplicate_reason="GEOGRAPHY:duplicate_or_empty_RADIO_REF_ID",
            )
            vivienda_rows = _iter_rows(
                source / "VIVIENDA.csv",
                ("VIVIENDA_REF_ID", "RADIO_REF_ID"),
                "VIVIENDA",
            )
            vivienda_count = _execute_unique_rows(
                conn,
                "INSERT INTO vivienda(viv, radio) VALUES (?, ?)",
                (
                    (row["VIVIENDA_REF_ID"], row["RADIO_REF_ID"])
                    for row in vivienda_rows
                ),
                duplicate_reason="VIVIENDA:duplicate_or_empty_VIVIENDA_REF_ID",
            )
            hogar_rows = _iter_rows(
                source / "HOGAR.csv",
                ("HOGAR_REF_ID", "VIVIENDA_REF_ID"),
                "HOGAR",
            )
            household_count = _execute_unique_rows(
                conn,
                "INSERT INTO hogar(hh, viv) VALUES (?, ?)",
                (
                    (row["HOGAR_REF_ID"], row["VIVIENDA_REF_ID"])
                    for row in hogar_rows
                ),
                duplicate_reason="HOGAR:duplicate_or_empty_HOGAR_REF_ID",
            )
            conn.commit()

            unresolved_radios = conn.execute(
                "SELECT COUNT(*) FROM vivienda v LEFT JOIN geo g ON g.radio=v.radio WHERE g.radio IS NULL"
            ).fetchone()[0]
            missing_viviendas = conn.execute(
                "SELECT COUNT(*) FROM hogar h LEFT JOIN vivienda v ON v.viv=h.viv WHERE v.viv IS NULL"
            ).fetchone()[0]
            if unresolved_radios or missing_viviendas:
                raise DonorFrameError(
                    "donor_relational_failure:"
                    f"missing_viviendas={missing_viviendas}:"
                    f"missing_radios={unresolved_radios}"
                )

            conn.execute(
                "CREATE TABLE hh_dept AS "
                "SELECT h.hh AS hh, g.dept AS dept "
                "FROM hogar h JOIN vivienda v ON v.viv=h.viv JOIN geo g ON g.radio=v.radio"
            )
            conn.execute("CREATE UNIQUE INDEX hh_dept_pk ON hh_dept(hh)")
            conn.commit()

            person_rows = _iter_rows(
                source / "PERSONA.csv",
                ("PERSONA_REF_ID", "HOGAR_REF_ID", "P02", "P03"),
                "PERSONA",
            )
            person_count = 0
            person_batch: list[tuple[str]] = []
            household_batch: Counter[str] = Counter()
            for row in person_rows:
                person_id = row["PERSONA_REF_ID"]
                household_id = row["HOGAR_REF_ID"]
                if not person_id or not household_id:
                    raise DonorFrameError("PERSONA:empty_identity_or_household")
                person_batch.append((person_id,))
                household_batch[household_id] += 1
                person_count += 1
                if len(person_batch) >= 50000:
                    try:
                        conn.executemany(
                            "INSERT INTO person_id(person) VALUES (?)",
                            person_batch,
                        )
                    except sqlite3.IntegrityError as exc:
                        raise DonorFrameError(
                            "PERSONA:duplicate_PERSONA_REF_ID"
                        ) from exc
                    conn.executemany(
                        "INSERT INTO hh_counts(hh,n) VALUES (?,?) "
                        "ON CONFLICT(hh) DO UPDATE SET n=n+excluded.n",
                        household_batch.items(),
                    )
                    conn.commit()
                    person_batch.clear()
                    household_batch.clear()
            if person_batch:
                try:
                    conn.executemany(
                        "INSERT INTO person_id(person) VALUES (?)",
                        person_batch,
                    )
                except sqlite3.IntegrityError as exc:
                    raise DonorFrameError("PERSONA:duplicate_PERSONA_REF_ID") from exc
                conn.executemany(
                    "INSERT INTO hh_counts(hh,n) VALUES (?,?) "
                    "ON CONFLICT(hh) DO UPDATE SET n=n+excluded.n",
                    household_batch.items(),
                )
                conn.commit()

            orphan_person_households = conn.execute(
                "SELECT COUNT(*) FROM hh_counts p LEFT JOIN hogar h ON h.hh=p.hh WHERE h.hh IS NULL"
            ).fetchone()[0]
            if orphan_person_households:
                raise DonorFrameError(
                    "donor_relational_failure:"
                    f"orphan_person_households={orphan_person_households}"
                )
            masses = [
                (str(dept), int(mass))
                for dept, mass in conn.execute(
                    "SELECT d.dept, SUM(c.n) "
                    "FROM hh_counts c JOIN hh_dept d ON d.hh=c.hh "
                    "GROUP BY d.dept ORDER BY d.dept"
                )
            ]
            if sum(mass for _, mass in masses) != person_count:
                raise DonorFrameError("donor_person_mass_does_not_sum_to_person_count")
        finally:
            conn.close()

        mass_path = staging / "donor_person_mass.csv"
        _write_mass(mass_path, masses)
        qa = {
            "frame_vintage": 2010,
            "geography_rows": geography_count,
            "dwellings": vivienda_count,
            "households": household_count,
            "persons": person_count,
            "departments": len(masses),
            "person_to_household": "validated",
            "household_to_vivienda": "validated",
            "vivienda_to_radio_geography": "validated",
            "person_identity_unique": True,
            "household_identity_unique": True,
            "vivienda_identity_unique": True,
            "radio_identity_unique": True,
            "donor_person_mass_total": sum(mass for _, mass in masses),
        }
        (staging / "qa.json").write_text(_canonical_json(qa), encoding="utf-8")
        manifest = {
            "contract": CONTRACT,
            "release_id": release_id,
            "status": "local_authorized_metadata_lock",
            "frame_vintage": 2010,
            "source_files": source_meta,
            "geography": geography_meta,
            "identity_graph": [
                "PERSONA_REF_ID -> HOGAR_REF_ID",
                "HOGAR_REF_ID -> VIVIENDA_REF_ID",
                "VIVIENDA_REF_ID -> RADIO_REF_ID",
                "RADIO_REF_ID -> department_2010_id",
            ],
            "artifacts": {
                "donor_person_mass.csv": {
                    "sha256": _sha256(mass_path),
                    "size_bytes": mass_path.stat().st_size,
                },
                "qa.json": {
                    "sha256": _sha256(staging / "qa.json"),
                    "size_bytes": (staging / "qa.json").stat().st_size,
                },
            },
            "qa": qa,
            "custody": {
                "microdata_payload_republished": False,
                "local_absolute_paths_recorded": False,
                "source_use": "authorized local input only",
            },
            "limitations": [
                "This lock proves identity and relational integrity of the supplied local donor frame; it does not grant or restate redistribution rights.",
                "No Census microdata rows are copied into the lock artifact.",
                "The first CI proof uses a bounded fixture; a real release requires running this same lock against the authorized complete CPV-2010 input.",
            ],
        }
        (staging / "manifest.json").write_text(
            _canonical_json(manifest), encoding="utf-8"
        )
        db_path.unlink(missing_ok=True)
        staging.replace(destination)
        return destination
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def validate_donor_frame_lock(
    lock_root: Path,
    databasepath: Path,
    *,
    geography_path: Path | None = None,
) -> dict[str, object]:
    """Verify that local donor bytes still match an existing metadata lock."""
    root = Path(lock_root).expanduser().resolve()
    source = Path(databasepath).expanduser().resolve()
    geography = (
        Path(geography_path).expanduser().resolve()
        if geography_path is not None
        else source / "GEOGRAPHY.csv"
    )
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DonorFrameError("donor_lock_manifest_missing_or_invalid") from exc
    if manifest.get("contract") != CONTRACT:
        raise DonorFrameError("unexpected_donor_lock_contract")
    for name in SOURCE_FILES:
        path = source / name
        expected = ((manifest.get("source_files") or {}).get(name) or {}).get("sha256")
        if not path.is_file() or expected != _sha256(path):
            raise DonorFrameError(f"donor_lock_source_hash_mismatch:{name}")
    expected_geography = (manifest.get("geography") or {}).get("sha256")
    if not geography.is_file() or expected_geography != _sha256(geography):
        raise DonorFrameError("donor_lock_geography_hash_mismatch")
    mass_path = root / "donor_person_mass.csv"
    expected_mass = (
        ((manifest.get("artifacts") or {}).get("donor_person_mass.csv") or {}).get(
            "sha256"
        )
    )
    if not mass_path.is_file() or expected_mass != _sha256(mass_path):
        raise DonorFrameError("donor_lock_mass_artifact_hash_mismatch")
    return manifest
