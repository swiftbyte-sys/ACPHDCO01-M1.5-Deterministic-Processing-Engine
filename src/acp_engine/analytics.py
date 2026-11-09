"""Reproducible longitudinal and panel-data analytics for the M2.5 lab.

The path model is an observed-variable mediation model (SEM-style path analysis),
not a latent-variable covariance SEM. The distinction is intentional and is
reported in every result so that the implementation does not overclaim.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

REQUIRED_COLUMNS = {
    "team_id", "period", "ai_adoption", "test_coverage", "resolution_hours"
}


class PanelDataError(ValueError):
    """Raised when panel data violate the declared analysis contract."""


@dataclass(frozen=True)
class Coefficient:
    estimate: float
    std_error: float
    ci95_low: float
    ci95_high: float
    p_value: float


@dataclass(frozen=True)
class AnalysisResult:
    data_kind: str
    observations: int
    entities: int
    periods: int
    panel_regression: dict[str, Any]
    observed_path_model: dict[str, Any]
    limitations: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_panel(data: pd.DataFrame) -> pd.DataFrame:
    """Validate and return a defensive copy of a team-by-period panel."""
    missing = REQUIRED_COLUMNS.difference(data.columns)
    if missing:
        raise PanelDataError(f"missing required columns: {sorted(missing)}")
    frame = data.loc[:, sorted(REQUIRED_COLUMNS)].copy()
    if frame.empty:
        raise PanelDataError("panel dataset is empty")
    if frame[["team_id", "period"]].isna().any().any():
        raise PanelDataError("team_id and period cannot be null")
    if frame.duplicated(["team_id", "period"]).any():
        raise PanelDataError("duplicate entity-period observations are not allowed")
    numeric = ["ai_adoption", "test_coverage", "resolution_hours"]
    for col in numeric:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    values = frame[numeric].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise PanelDataError("numeric fields must be finite and non-null")
    for col in ("ai_adoption", "test_coverage"):
        if not frame[col].between(0, 1).all():
            raise PanelDataError(f"{col} must be between 0 and 1")
    if (frame["resolution_hours"] <= 0).any():
        raise PanelDataError("resolution_hours must be positive")
    if frame["team_id"].nunique() < 3:
        raise PanelDataError("at least three entities are required for panel analysis")
    if frame["period"].nunique() < 2:
        raise PanelDataError("at least two periods are required for longitudinal analysis")
    return frame.sort_values(["team_id", "period"]).reset_index(drop=True)


def generate_synthetic_panel(
    teams: int = 24, periods: int = 12, seed: int = 20261109
) -> pd.DataFrame:
    """Generate explicitly synthetic repeated observations for reproducible tests."""
    if teams < 3 or periods < 2:
        raise ValueError("teams must be >=3 and periods must be >=2")
    rng = np.random.default_rng(seed)
    team_effects = rng.normal(0, 3.0, teams)
    adoption_offsets = rng.normal(0, 0.08, teams)
    rows: list[dict[str, Any]] = []
    for team in range(teams):
        for period in range(periods):
            adoption = np.clip(
                0.08 + 0.055 * period + adoption_offsets[team] + rng.normal(0, 0.055),
                0.0, 1.0,
            )
            coverage = np.clip(
                0.32 + 0.38 * adoption + 0.012 * period
                + 0.035 * team_effects[team] + rng.normal(0, 0.045),
                0.05, 0.99,
            )
            resolution = (
                52.0 - 13.0 * adoption - 18.0 * coverage
                + team_effects[team] - 0.22 * period + rng.normal(0, 2.0)
            )
            rows.append({
                "team_id": f"team_{team + 1:02d}",
                "period": period + 1,
                "ai_adoption": float(adoption),
                "test_coverage": float(coverage),
                "resolution_hours": float(max(1.0, resolution)),
            })
    return pd.DataFrame(rows)


def _coefficient(model: Any, name: str) -> Coefficient:
    ci = model.conf_int().loc[name]
    return Coefficient(
        estimate=float(model.params[name]),
        std_error=float(model.bse[name]),
        ci95_low=float(ci.iloc[0]),
        ci95_high=float(ci.iloc[1]),
        p_value=float(model.pvalues[name]),
    )


def _path_estimates(frame: pd.DataFrame) -> tuple[float, float, float, float, float]:
    # a: AI adoption -> test coverage; b and c': coverage/adoption -> resolution.
    a_model = smf.ols(
        "test_coverage ~ ai_adoption + C(team_id) + C(period)", data=frame
    ).fit()
    outcome_model = smf.ols(
        "resolution_hours ~ test_coverage + ai_adoption + C(team_id) + C(period)",
        data=frame,
    ).fit()
    a = float(a_model.params["ai_adoption"])
    b = float(outcome_model.params["test_coverage"])
    direct = float(outcome_model.params["ai_adoption"])
    return a, b, direct, a * b, direct + a * b


def _cluster_bootstrap_indirect(
    frame: pd.DataFrame, *, replicates: int, seed: int
) -> tuple[float, float]:
    rng = np.random.default_rng(seed)
    entities = frame["team_id"].unique()
    estimates: list[float] = []
    for _ in range(replicates):
        sampled = rng.choice(entities, size=len(entities), replace=True)
        chunks = []
        for draw, entity in enumerate(sampled):
            chunk = frame.loc[frame["team_id"] == entity].copy()
            chunk["team_id"] = f"boot_{draw:03d}"
            chunks.append(chunk)
        sample = pd.concat(chunks, ignore_index=True)
        try:
            _, _, _, indirect, _ = _path_estimates(sample)
            if np.isfinite(indirect):
                estimates.append(indirect)
        except (ValueError, np.linalg.LinAlgError):
            continue
    if len(estimates) < max(20, int(replicates * 0.5)):
        raise PanelDataError("too few valid cluster-bootstrap replications")
    low, high = np.quantile(estimates, [0.025, 0.975])
    return float(low), float(high)


def analyze_panel(
    data: pd.DataFrame, *, bootstrap_replicates: int = 200, seed: int = 2026,
    data_kind: str = "user_supplied",
) -> AnalysisResult:
    """Fit entity/time fixed-effects panel regression and observed path analysis."""
    if bootstrap_replicates < 0:
        raise ValueError("bootstrap_replicates cannot be negative")
    frame = validate_panel(data)
    panel = smf.ols(
        "resolution_hours ~ ai_adoption + test_coverage + C(team_id) + C(period)",
        data=frame,
    ).fit(cov_type="cluster", cov_kwds={"groups": frame["team_id"]})
    adoption = _coefficient(panel, "ai_adoption")
    coverage = _coefficient(panel, "test_coverage")
    a, b, direct, indirect, total = _path_estimates(frame)
    if bootstrap_replicates:
        indirect_low, indirect_high = _cluster_bootstrap_indirect(
            frame, replicates=bootstrap_replicates, seed=seed
        )
    else:
        indirect_low = indirect_high = None
    return AnalysisResult(
        data_kind=data_kind,
        observations=len(frame),
        entities=frame["team_id"].nunique(),
        periods=frame["period"].nunique(),
        panel_regression={
            "outcome": "resolution_hours",
            "specification": "entity and period fixed effects; standard errors clustered by entity",
            "r_squared": float(panel.rsquared),
            "ai_adoption": asdict(adoption),
            "test_coverage": asdict(coverage),
        },
        observed_path_model={
            "model_type": "observed-variable mediation path analysis; not latent-variable SEM",
            "path_a_ai_adoption_to_coverage": a,
            "path_b_coverage_to_resolution": b,
            "direct_effect_ai_adoption_to_resolution": direct,
            "indirect_effect_a_times_b": indirect,
            "total_effect_direct_plus_indirect": total,
            "indirect_effect_cluster_bootstrap_ci95": [indirect_low, indirect_high],
            "bootstrap_replicates_requested": bootstrap_replicates,
        },
        limitations=[
            "Synthetic results are software-validation evidence, not evidence about real organizations."
            if data_kind == "synthetic" else
            "Observational panel associations do not establish causality without defensible identification assumptions.",
            "The path model uses observed variables and does not estimate latent constructs or a measurement model.",
            "Fixed effects do not remove time-varying confounding, reverse causality, or measurement error.",
            "Cluster-robust inference can be unreliable with few entities; report the number of clusters.",
            "The panel must be assessed for missingness, selection, and common-shock assumptions before substantive interpretation.",
        ],
    )
