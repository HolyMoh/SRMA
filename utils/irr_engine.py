"""
Inter-rater reliability (IRR) computation engine.

IRRResult is imported from models/extraction_schema.py — not defined here.
Import direction: models.extraction_schema, config, standard library + sklearn.

config.IRR_MINIMUM_N guard: returns insufficient_n=True if n < IRR_MINIMUM_N.
"""
from __future__ import annotations
from typing import Any

import math
import config
from models.extraction_schema import IRRResult


def _kappa_interpretation(kappa: float) -> str:
    """Landis-Koch kappa interpretation bands."""
    if kappa < 0.0:
        return "poor"
    elif kappa < 0.20:
        return "slight"
    elif kappa < 0.40:
        return "fair"
    elif kappa < 0.60:
        return "moderate"
    elif kappa < 0.80:
        return "substantial"
    else:
        return "almost perfect"


def compute_categorical_irr(
    variable: str,
    rater1_values: list[Any],
    rater2_values: list[Any],
) -> IRRResult:
    """Compute Cohen's Kappa for categorical variable agreement.

    Args:
        variable: canonical variable name
        rater1_values: list of string/categorical ratings from rater 1
        rater2_values: list of string/categorical ratings from rater 2
            (must be same length; None entries are counted as not_found)

    Returns:
        IRRResult with categorical fields populated
    """
    if len(rater1_values) != len(rater2_values):
        raise ValueError(
            f"rater1 ({len(rater1_values)}) and rater2 ({len(rater2_values)}) "
            f"must have equal length for variable '{variable}'"
        )

    n_total = len(rater1_values)
    match_count = 0
    mismatch_count = 0
    not_found_count = 0
    valid_pairs: list[tuple[str, str]] = []

    for v1, v2 in zip(rater1_values, rater2_values):
        if v1 is None or v2 is None:
            not_found_count += 1
        elif str(v1) == str(v2):
            match_count += 1
            valid_pairs.append((str(v1), str(v2)))
        else:
            mismatch_count += 1
            valid_pairs.append((str(v1), str(v2)))

    n_compared = match_count + mismatch_count

    if n_compared < config.IRR_MINIMUM_N:
        return IRRResult(
            variable=variable,
            variable_type="categorical",
            n_compared=n_compared,
            insufficient_n=True,
            insufficient_n_warning=(
                f"Only {n_compared} comparable pairs found for '{variable}'. "
                f"Minimum required: {config.IRR_MINIMUM_N} (config.IRR_MINIMUM_N)."
            ),
            match_count=match_count,
            mismatch_count=mismatch_count,
            not_found_count=not_found_count,
        )

    # Cohen's Kappa via sklearn
    from sklearn.metrics import cohen_kappa_score
    y1 = [p[0] for p in valid_pairs]
    y2 = [p[1] for p in valid_pairs]
    try:
        kappa = float(cohen_kappa_score(y1, y2))
    except Exception:
        kappa = 0.0

    # SE: sqrt((Po*(1-Po)) / (n*(1-Pe)^2))
    # Approximate Po from observed agreement
    n = len(valid_pairs)
    po = match_count / n if n > 0 else 0.0
    # Approximate Pe from marginal frequencies
    from collections import Counter
    c1 = Counter(y1)
    c2 = Counter(y2)
    all_cats = set(c1.keys()) | set(c2.keys())
    pe = sum(
        (c1.get(cat, 0) / n) * (c2.get(cat, 0) / n)
        for cat in all_cats
    ) if n > 0 else 0.0
    denom = n * (1 - pe) ** 2 if pe < 1 else 1e-9
    kappa_se = math.sqrt(po * (1 - po) / denom) if denom > 0 else None

    return IRRResult(
        variable=variable,
        variable_type="categorical",
        n_compared=n_compared,
        insufficient_n=False,
        cohen_kappa=kappa,
        kappa_se=kappa_se,
        kappa_interpretation=_kappa_interpretation(kappa),
        match_count=match_count,
        mismatch_count=mismatch_count,
        not_found_count=not_found_count,
    )


def compute_continuous_irr(
    variable: str,
    rater1_values: list[Any],
    rater2_values: list[Any],
    agreement_tolerance: float = 0.01,
) -> IRRResult:
    """Compute percentage agreement and MAE for continuous variable agreement.

    Args:
        variable: canonical variable name
        rater1_values: list of numeric values from rater 1
        rater2_values: list of numeric values from rater 2
        agreement_tolerance: relative tolerance for match (default 1%)

    Returns:
        IRRResult with continuous fields populated
    """
    if len(rater1_values) != len(rater2_values):
        raise ValueError(
            f"rater1 ({len(rater1_values)}) and rater2 ({len(rater2_values)}) "
            f"must have equal length for variable '{variable}'"
        )

    match_count = 0
    mismatch_count = 0
    not_found_count = 0
    abs_errors: list[float] = []

    for v1, v2 in zip(rater1_values, rater2_values):
        if v1 is None or v2 is None:
            not_found_count += 1
            continue
        try:
            f1, f2 = float(v1), float(v2)
        except (TypeError, ValueError):
            not_found_count += 1
            continue

        abs_err = abs(f1 - f2)
        abs_errors.append(abs_err)
        denom = max(abs(f2), 1e-9)
        rel_diff = abs_err / denom
        if rel_diff <= agreement_tolerance:
            match_count += 1
        else:
            mismatch_count += 1

    n_compared = match_count + mismatch_count

    if n_compared < config.IRR_MINIMUM_N:
        return IRRResult(
            variable=variable,
            variable_type="continuous",
            n_compared=n_compared,
            insufficient_n=True,
            insufficient_n_warning=(
                f"Only {n_compared} comparable pairs found for '{variable}'. "
                f"Minimum required: {config.IRR_MINIMUM_N} (config.IRR_MINIMUM_N)."
            ),
            match_count=match_count,
            mismatch_count=mismatch_count,
            not_found_count=not_found_count,
        )

    pct_agreement = (match_count / n_compared * 100.0) if n_compared > 0 else 0.0
    mae = (sum(abs_errors) / len(abs_errors)) if abs_errors else None
    max_ae = max(abs_errors) if abs_errors else None

    return IRRResult(
        variable=variable,
        variable_type="continuous",
        n_compared=n_compared,
        insufficient_n=False,
        percentage_agreement=pct_agreement,
        mean_absolute_error=mae,
        max_absolute_error=max_ae,
        match_count=match_count,
        mismatch_count=mismatch_count,
        not_found_count=not_found_count,
    )


def compute_irr(
    variable: str,
    variable_type: str,
    rater1_values: list[Any],
    rater2_values: list[Any],
    agreement_tolerance: float = 0.01,
) -> IRRResult:
    """Dispatch IRR computation based on variable_type.

    Args:
        variable: canonical variable name
        variable_type: 'categorical' or 'continuous'
        rater1_values: rater 1 value list
        rater2_values: rater 2 value list
        agreement_tolerance: for continuous variables (default 1%)

    Returns:
        IRRResult
    """
    if variable_type == "categorical":
        return compute_categorical_irr(variable, rater1_values, rater2_values)
    elif variable_type == "continuous":
        return compute_continuous_irr(
            variable, rater1_values, rater2_values, agreement_tolerance
        )
    else:
        raise ValueError(
            f"Unknown variable_type '{variable_type}'. "
            "Expected 'categorical' or 'continuous'."
        )
