import numpy as np
import pandas as pd
import pytest

from acp_engine.analytics import (
    PanelDataError, analyze_panel, generate_synthetic_panel, validate_panel,
)


def test_synthetic_panel_shape_and_reproducibility():
    a = generate_synthetic_panel(teams=8, periods=5, seed=7)
    b = generate_synthetic_panel(teams=8, periods=5, seed=7)
    pd.testing.assert_frame_equal(a, b)
    assert len(a) == 40
    assert a.team_id.nunique() == 8
    assert a.period.nunique() == 5


def test_panel_validation_rejects_duplicate_entity_period():
    data = generate_synthetic_panel(teams=4, periods=3)
    duplicated = pd.concat([data, data.iloc[[0]]], ignore_index=True)
    with pytest.raises(PanelDataError, match="duplicate entity-period"):
        validate_panel(duplicated)


def test_panel_validation_rejects_invalid_ranges():
    data = generate_synthetic_panel(teams=4, periods=3)
    data.loc[0, "ai_adoption"] = 1.5
    with pytest.raises(PanelDataError, match="between 0 and 1"):
        validate_panel(data)


def test_panel_validation_rejects_non_finite_values():
    data = generate_synthetic_panel(teams=4, periods=3)
    data.loc[0, "resolution_hours"] = np.inf
    with pytest.raises(PanelDataError, match="finite"):
        validate_panel(data)


def test_analysis_returns_panel_and_path_results():
    data = generate_synthetic_panel(teams=10, periods=6, seed=11)
    result = analyze_panel(data, bootstrap_replicates=30, seed=12, data_kind="synthetic")
    payload = result.to_dict()
    assert payload["observations"] == 60
    assert payload["entities"] == 10
    assert "ai_adoption" in payload["panel_regression"]
    assert payload["observed_path_model"]["model_type"].startswith("observed-variable")
    assert len(payload["observed_path_model"]["indirect_effect_cluster_bootstrap_ci95"]) == 2
    assert "not evidence about real organizations" in payload["limitations"][0]


def test_analysis_refuses_single_period():
    data = generate_synthetic_panel(teams=5, periods=2)
    data = data[data.period == 1]
    with pytest.raises(PanelDataError, match="at least two periods"):
        analyze_panel(data, bootstrap_replicates=0)
