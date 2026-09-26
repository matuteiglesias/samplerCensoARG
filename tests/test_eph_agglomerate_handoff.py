from __future__ import annotations

import pandas as pd
import pytest

from censo_sampler.eph_agglomerate_handoff import (
    EphAgglomerateHandoffError,
    derive_household_agglomerates,
)


def _selection() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sample_household_id": "s1",
                "frame_household_id": "h1",
                "department_id": "02001",
                "radio_id": "020010101",
                "household_person_count": 2,
                "selection_probability": 0.5,
                "design_inverse_probability_weight": 2.0,
            },
            {
                "sample_household_id": "s2",
                "frame_household_id": "h2",
                "department_id": "06028",
                "radio_id": "060280101",
                "household_person_count": 3,
                "selection_probability": 0.25,
                "design_inverse_probability_weight": 4.0,
            },
            {
                "sample_household_id": "s3",
                "frame_household_id": "h3",
                "department_id": "50007",
                "radio_id": "500070101",
                "household_person_count": 1,
                "selection_probability": 1.0,
                "design_inverse_probability_weight": 1.0,
            },
        ]
    )


def _a7() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "radio_2010_id": "020010101",
                "department_2010_id": "02001",
                "province_2010_id": "02",
                "eph_agglomerate_id": "32",
            },
            {
                "radio_2010_id": "060280101",
                "department_2010_id": "06028",
                "province_2010_id": "06",
                "eph_agglomerate_id": "33",
            },
        ]
    )


def test_left_join_preserves_outside_eph_frame_households():
    out, qa = derive_household_agglomerates(_selection(), _a7())

    assert out.sample_household_id.tolist() == ["s1", "s2", "s3"]
    assert out.eph_agglomerate_id.tolist()[:2] == ["32", "33"]
    assert pd.isna(out.eph_agglomerate_id.iloc[2])
    assert out.mapped_to_eph_frame.tolist() == [True, True, False]
    assert out.outside_eph_frame.tolist() == [False, False, True]
    assert qa["input_households"] == qa["output_households"] == 3
    assert qa["zero_households_dropped"] is True
    assert qa["represented_eph_agglomerate_ids"] == ["32", "33"]
    assert qa["mapped_selected_persons"] == 5
    assert qa["outside_eph_frame_selected_persons"] == 1
    assert qa["mapped_design_person_mass"] == pytest.approx(16.0)
    assert qa["outside_eph_frame_design_person_mass"] == pytest.approx(1.0)
    assert qa["redistribution_applied"] is False
    assert qa["imputation_applied"] is False
    assert qa["selection_changed"] is False
    assert qa["weight_changed"] is False


def test_mapped_department_identity_must_agree():
    a7 = _a7()
    a7.loc[a7.radio_2010_id == "060280101", "department_2010_id"] = "06035"

    with pytest.raises(
        EphAgglomerateHandoffError,
        match="mapped_radio_department_identity_mismatch",
    ):
        derive_household_agglomerates(_selection(), a7)


def test_a7_relation_must_be_functional_by_radio():
    a7 = pd.concat([_a7(), _a7().iloc[[0]]], ignore_index=True)

    with pytest.raises(
        EphAgglomerateHandoffError,
        match="a7_radio_identity_not_unique",
    ):
        derive_household_agglomerates(_selection(), a7)


def test_numeric_or_short_radio_identity_is_rejected():
    selection = _selection()
    selection.loc[0, "radio_id"] = "20010101"

    with pytest.raises(
        EphAgglomerateHandoffError,
        match="selection_radio_id_must_be_9_digits",
    ):
        derive_household_agglomerates(selection, _a7())
