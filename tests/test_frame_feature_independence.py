from pathlib import Path

import pyarrow.parquet as pq

from censo_sampler.frame_2010 import build_cpv2010_frame
from censo_sampler.frame_contract import validate_frame
from censo_sampler.release_v2 import build_sample_release_v2, validate_sample_release_v2


def _minimal_source(root: Path) -> Path:
    root.mkdir()
    (root / "VIVIENDA.csv").write_text(
        "VIVIENDA_REF_ID;RADIO_REF_ID;ARBITRARY_V\n"
        "v1;r1;foo\n"
        "v2;r1;bar\n",
        encoding="utf-8",
    )
    (root / "HOGAR.csv").write_text(
        "HOGAR_REF_ID;VIVIENDA_REF_ID;ARBITRARY_H\n"
        "h1;v1;alpha\n"
        "h2;v2;beta\n",
        encoding="utf-8",
    )
    # Deliberately no P02/P03/EDAD or any EPH-facing feature.
    (root / "PERSONA.csv").write_text(
        "PERSONA_REF_ID;HOGAR_REF_ID;UNRELATED_SOURCE_FIELD\n"
        "p1;h1;x\n"
        "p2;h1;y\n"
        "p3;h2;z\n",
        encoding="utf-8",
    )
    (root / "GEOGRAPHY.csv").write_text(
        "RADIO_REF_ID,radio_2010_id,department_2010_id\n"
        "r1,060010101,06001\n",
        encoding="utf-8",
    )
    return root


def test_frame_and_sampler_need_no_eph_selected_columns(tmp_path: Path) -> None:
    source = _minimal_source(tmp_path / "source")
    frame = build_cpv2010_frame(source, tmp_path / "frames")
    checked = validate_frame(frame)
    assert checked["counts"]["persons"] == 3

    person_schema = set(
        pq.read_schema(frame / "payload/persona.parquet").names
    )
    assert "P02" not in person_schema
    assert "P03" not in person_schema
    assert "EDAD" not in person_schema
    assert "UNRELATED_SOURCE_FIELD" in person_schema

    target = tmp_path / "target.csv"
    target.write_text(
        "department_id,target_year,target_person_mass\n06001,2024,3\n",
        encoding="utf-8",
    )
    release = build_sample_release_v2(
        frame,
        tmp_path / "samples",
        target_population=target,
        target_year=2024,
        fraction=1.0,
    )
    result = validate_sample_release_v2(release)
    assert result["households"] == 2
    assert result["persons"] == 3
    materialized_schema = set(pq.read_schema(release / "persona.parquet").names)
    assert "UNRELATED_SOURCE_FIELD" in materialized_schema
