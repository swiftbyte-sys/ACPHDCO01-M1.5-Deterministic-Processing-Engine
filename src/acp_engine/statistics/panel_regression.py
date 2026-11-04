"""Pooled, fixed-effects, and random-effects panel regression adapters."""
from __future__ import annotations

from typing import Any
import pandas as pd


class PanelModelError(ValueError):
    """Raised for invalid panel data or estimation failures."""


def fit_panel_models(
    data: pd.DataFrame,
    *,
    outcome: str,
    entity: str,
    time: str,
    regressors: list[str],
) -> dict[str, Any]:
    """Estimate pooled OLS, entity fixed effects, and random effects.

    Uses linearmodels. Entity-clustered covariance is requested for pooled and
    fixed-effects models. The function returns estimates, not an automatic
    causal interpretation or an unconditional model-selection decision.
    """
    if not regressors:
        raise PanelModelError("at least one regressor is required")
    required = [outcome, entity, time, *regressors]
    missing_cols = [col for col in required if col not in data.columns]
    if missing_cols:
        raise PanelModelError(f"missing columns: {missing_cols}")
    frame = data[required].dropna().copy()
    if frame.empty:
        raise PanelModelError("no complete observations remain")
    if frame[entity].nunique() < 2 or frame[time].nunique() < 2:
        raise PanelModelError("panel data require at least two entities and two time periods")

    try:
        from linearmodels.panel import PooledOLS, PanelOLS, RandomEffects
    except ImportError as exc:
        raise ImportError("Panel regression requires linearmodels; install requirements-m2.5.txt") from exc

    panel = frame.set_index([entity, time]).sort_index()
    y = panel[outcome]
    x = panel[regressors]
    # Add an explicit constant for pooled OLS and random effects.
    import statsmodels.api as sm
    x_const = sm.add_constant(x, has_constant="add")
    try:
        pooled = PooledOLS(y, x_const).fit(
            cov_type="clustered", cluster_entity=True
        )
        fixed = PanelOLS(y, x, entity_effects=True, time_effects=True).fit(
            cov_type="clustered", cluster_entity=True
        )
        random = RandomEffects(y, x_const).fit(
            cov_type="clustered", cluster_entity=True
        )
    except Exception as exc:
        raise PanelModelError(f"panel estimation failed: {exc}") from exc

    def pack(result: Any) -> dict[str, Any]:
        return {
            "coefficients": {k: float(v) for k, v in result.params.items()},
            "standard_errors": {k: float(v) for k, v in result.std_errors.items()},
            "p_values": {k: float(v) for k, v in result.pvalues.items()},
            "r_squared": float(result.rsquared),
            "n_observations": int(result.nobs),
        }

    return {
        "method": "panel_regression",
        "entity_count": int(frame[entity].nunique()),
        "time_period_count": int(frame[time].nunique()),
        "n_observations": int(len(frame)),
        "pooled_ols": pack(pooled),
        "fixed_effects_entity_and_time": pack(fixed),
        "random_effects": pack(random),
        "limitations": [
            "Model choice depends on assumptions about unobserved effects and regressors.",
            "Clustered standard errors do not solve omitted-variable bias or endogeneity.",
            "A Hausman test is not implemented in this initial version."
        ],
    }
