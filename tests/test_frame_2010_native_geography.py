from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from censo_sampler.frame_2010 import build_cpv2010_frame
from censo_sampler.frame_contract import validate_frame
from censo_sampler.geography_2010 import inspect_geography_source

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


def _native_source(root: Path) -> tuple[Path, Path]:
    source = root / "Censo_2010"
    source.mkdir()
    shutil.copy2(FIXTURE / "HOGAR.csv", source / "HOGAR.csv")
    shutil.copy2(FIXTURE / "PERSONA.csv", source / "PERSONA.csv")

    vivienda = (FIXTURE / "VIVIENDA.csv").read_text(encoding="utf-8")
    for old, new in {"r001": "1", "r002": "2", "r003": "3", "r004": "4"}.items():
        vivienda = vivienda.replace(old, new)
    (source / "VIVIENDA.csv").write_text(vivienda, encoding="utf-8")

    geography = root / "GEO.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"PROV_REF_ID": 1, "CPV2010_REF_ID": 1, "IDPROV": 2, "DPTO_REF_ID": 1, "IDDPTO": 2001, "FRAC_REF_ID": 1, "RADIO_REF_ID": 1, "IDRADIO": 20010101},
                {"PROV_REF_ID": 2, "CPV2010_REF_ID": 1, "IDPROV": 50, "DPTO_REF_ID": 2, "IDDPTO": 50007, "FRAC_REF_ID": 2, "RADIO_REF_ID": 2, "IDRADIO": 500070201},
                {"PROV_REF_ID": 3, "CPV2010_REF_ID": 1, "IDPROV": 90, "DPTO_REF_ID": 3, "IDDPTO": 90084, "FRAC_REF_ID": 3, "RADIO_REF_ID": 3, "IDRADIO": 900840301},
                {"PROV_REF_ID": 4, "CPV2010_REF_ID": 1, "IDPROV": 94, "DPTO_REF_ID": 4, "IDDPTO": 94008, "FRAC_REF_ID": 4, "RADIO_REF_ID": 4, "IDRADIO": 940080101},
            ]
        ),
        geography,
    )
    return source, geography


def test_native_geo_parquet_maps_iddpto_and_idradio(tmp_path: Path) -> None:
    source, geography = _native_source(tmp_path)
    profile = inspect_geography_source(geography)
    assert profile["radio_field"] == "IDRADIO"
    assert profile["department_field"] == "IDDPTO"

    frame = build_cpv2010_frame(
        source,
        tmp_path / "frames",
        geography_path=geography,
    )
    checked = validate_frame(frame)
    assert checked["status"] == "valid"
    assert checked["counts"] == {
        "dwellings": 6,
        "households": 6,
        "persons": 10,
        "departments": 4,
    }

    hh = pq.read_table(frame / "frame_households.parquet").to_pylist()
    assert {row["department_id"] for row in hh} == {
        "02001",
        "50007",
        "90084",
        "94008",
    }
    assert {row["radio_id"] for row in hh} == {
        "020010101",
        "500070201",
        "900840301",
        "940080101",
    }
