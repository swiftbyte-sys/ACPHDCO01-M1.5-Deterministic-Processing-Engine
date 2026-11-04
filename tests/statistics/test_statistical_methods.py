"""Smoke tests for M2.5 statistical analysis modules.

These tests verify API execution on controlled synthetic data. They do not
replace a substantive statistical validation or parameter-recovery study.
"""
import numpy as np
import pandas as pd
import pytest

from acp_engine.statistics.sem import fit_sem, StatisticalModelError
from acp_engine.statistics.longitudinal import (
    fit_longitudinal_mixed_model, LongitudinalModelError,
)
from acp_engine.statistics.panel_regression import (
    fit_panel_models, PanelModelError,
)


def test_sem_smoke():
    rng = np.random.default_rng(42)
    latent = rng.normal(size=500)
    data = pd.DataFrame({
        "x1": latent + rng.normal(0, 0.35, 500),
        "x2": 0.8 * latent + rng.normal(0, 0.35, 500),
        "x3": 1.1 * latent + rng.normal(0, 0.35, 500),
        "outcome": 0.7 * latent + rng.normal(0, 0.5, 500),
    })
    result = fit_sem(data, "F =~ x1 + x2 + x3\noutcome ~ F")
    assert result["method"] == "SEM"
    assert result["n_observations"] == 500
    assert result["converged"] is True
    assert result["estimates"]


def test_sem_rejects_empty_data():
    with pytest.raises(StatisticalModelError):
        fit_sem(pd.DataFrame(), "F =~ x1 + x2 + x3")


def test_longitudinal_mixed_model_smoke():
    rng = np.random.default_rng(43)
    rows = []
    for entity in range(30):
        intercept = rng.normal(0, 1)
        for month in range(6):
            rows.append({
                "entity": entity,
                "month": month,
                "efficiency": intercept + 0.25 * month + rng.normal(0, 0.3),
                "team_size": 5 + entity % 4,
            })
    result = fit_longitudinal_mixed_model(
        pd.DataFrame(rows), outcome="efficiency", entity="entity",
        time="month", fixed_effects=["team_size"],
    )
    assert result["method"] == "longitudinal_random_intercept_mixed_model"
    assert result["n_entities"] == 30
    assert result["converged"] is True
    assert "Q(\"month\")" in result["formula"]


def test_longitudinal_rejects_missing_columns():
    with pytest.raises(LongitudinalModelError):
        fit_longitudinal_mixed_model(
            pd.DataFrame({"entity": [1, 1], "month": [1, 2]}),
            outcome="y", entity="entity", time="month",
        )


def test_panel_models_smoke():
    rng = np.random.default_rng(44)
    rows = []
    entity_effects = rng.normal(0, 1, 24)
    for entity in range(24):
        for period in range(8):
            x = rng.normal()
            y = 1.7 * x + entity_effects[entity] + 0.15 * period + rng.normal(0, 0.25)
            rows.append({"entity": entity, "period": period, "x": x, "y": y})
    result = fit_panel_models(
        pd.DataFrame(rows), outcome="y", entity="entity",
        time="period", regressors=["x"],
    )
    assert result["method"] == "panel_regression"
    assert result["n_observations"] == 192
    assert "fixed_effects_entity_and_time" in result
    assert "random_effects" in result


def test_panel_rejects_no_regressors():
    with pytest.raises(PanelModelError):
        fit_panel_models(
            pd.DataFrame({"entity": [1, 2], "period": [1, 1], "y": [2, 3]}),
            outcome="y", entity="entity", time="period", regressors=[],
        )
