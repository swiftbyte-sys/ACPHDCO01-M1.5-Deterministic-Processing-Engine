"""Structural Equation Modeling adapter.

Requires semopy. The function returns model estimates and fit diagnostics;
it deliberately does not interpret statistical significance as causation.
"""
from __future__ import annotations

from typing import Any
import pandas as pd


class StatisticalModelError(ValueError):
    """Raised when a statistical model cannot be estimated or validated."""


def fit_sem(data: pd.DataFrame, model_description: str) -> dict[str, Any]:
    """Fit a SEM model described using semopy syntax.

    Example:
        measurement =~ indicator_1 + indicator_2 + indicator_3
        outcome ~ measurement

    Returns parameter estimates and available fit indices. Input data and
    model specification must be selected and justified by the researcher.
    """
    if not isinstance(data, pd.DataFrame) or data.empty:
        raise StatisticalModelError("data must be a non-empty pandas DataFrame")
    if not isinstance(model_description, str) or not model_description.strip():
        raise StatisticalModelError("model_description must be non-empty")
    if data.columns.duplicated().any():
        raise StatisticalModelError("dataset contains duplicate column names")

    try:
        from semopy import Model, calc_stats
    except ImportError as exc:
        raise ImportError("SEM support requires semopy; install requirements-m2.5.txt") from exc

    model = Model(model_description)
    try:
        model.fit(data)
        estimates = model.inspect()
        fit = calc_stats(model)
        solver_result = getattr(model, "last_result", None)
        converged = bool(getattr(solver_result, "success", False))
    except Exception as exc:
        raise StatisticalModelError(f"SEM estimation failed: {exc}") from exc

    fit_values = {}
    if hasattr(fit, "iloc") and len(fit.index):
        row = fit.iloc[0]
        for key in ("DoF", "chi2", "CFI", "TLI", "RMSEA", "AIC", "BIC", "LogLik"):
            if key in row.index:
                value = row[key]
                fit_values[key] = float(value) if pd.notna(value) else None

    return {
        "method": "SEM",
        "n_observations": int(len(data)),
        "model_description": model_description,
        "estimates": estimates.to_dict(orient="records"),
        "fit_indices": fit_values,
        "converged": converged,
        "limitations": [
            "Convergence does not establish model correctness or causal identification.",
            "Fit indices must be interpreted in light of estimator, sample size, identification, and data assumptions."
        ],
    }
