"""Longitudinal repeated-measures analysis using a mixed-effects model."""
from __future__ import annotations

from typing import Any
import pandas as pd


class LongitudinalModelError(ValueError):
    """Raised for invalid longitudinal data or estimation failures."""


def fit_longitudinal_mixed_model(
    data: pd.DataFrame,
    *,
    outcome: str,
    entity: str,
    time: str,
    fixed_effects: list[str] | None = None,
) -> dict[str, Any]:
    """Fit a random-intercept linear mixed model for repeated observations.

    Each entity must have repeated observations for longitudinal interpretation.
    Missing rows in model variables are dropped explicitly and counted.
    """
    required = [outcome, entity, time, *(fixed_effects or [])]
    missing_cols = [col for col in required if col not in data.columns]
    if missing_cols:
        raise LongitudinalModelError(f"missing columns: {missing_cols}")
    if data.empty:
        raise LongitudinalModelError("data must not be empty")

    frame = data[required].copy()
    original_n = len(frame)
    frame = frame.dropna()
    if frame.empty:
        raise LongitudinalModelError("no complete observations remain")
    if frame[entity].nunique() < 2:
        raise LongitudinalModelError("at least two entities are required")
    counts = frame.groupby(entity)[time].nunique()
    if not (counts >= 2).any():
        raise LongitudinalModelError("at least one entity must have repeated time observations")

    try:
        import statsmodels.formula.api as smf
        # Quote column names to support spaces and punctuation.
        def q(name: str) -> str:
            return 'Q("' + name.replace('"', '\\"') + '")'
        rhs = [q(time), *(q(col) for col in (fixed_effects or []))]
        formula = q(outcome) + " ~ " + " + ".join(rhs)
        fitted = smf.mixedlm(formula, frame, groups=frame[entity]).fit(reml=False)
    except Exception as exc:
        raise LongitudinalModelError(f"mixed-effects estimation failed: {exc}") from exc

    return {
        "method": "longitudinal_random_intercept_mixed_model",
        "n_observations_input": int(original_n),
        "n_observations_used": int(len(frame)),
        "n_entities": int(frame[entity].nunique()),
        "formula": formula,
        "coefficients": {k: float(v) for k, v in fitted.params.items()},
        "standard_errors": {k: float(v) for k, v in fitted.bse.items()},
        "p_values": {k: float(v) for k, v in fitted.pvalues.items()},
        "converged": bool(fitted.converged),
        "missing_rows_dropped": int(original_n - len(frame)),
        "limitations": [
            "This implementation uses a random-intercept Gaussian mixed model.",
            "Time is treated as a fixed-effect predictor; assess whether this matches the research design.",
            "Missing rows are listwise deleted; this is not a general missing-data solution."
        ],
    }
