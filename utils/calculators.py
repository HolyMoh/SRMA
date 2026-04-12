"""
Deterministic calculator for derived variables.

Uses sympy only. Returns (result, trace) or (None, "CALCULATION_FAILED: {reason}").
All CALCULATED variables require calculation_trace.

Import direction: imports config and utils.errors only (no LLM models).
"""
from __future__ import annotations
from typing import Any


def _safe_float(value: Any, name: str) -> tuple[float | None, str | None]:
    """Try converting value to float. Returns (float, None) or (None, error_msg)."""
    if value is None:
        return None, f"Input '{name}' is None"
    try:
        return float(value), None
    except (TypeError, ValueError) as e:
        return None, f"Input '{name}' cannot be converted to float: {e}"


def calculate_nnt_from_arr(
    arr: Any,
) -> tuple[float | None, str]:
    """NNT = 1 / ARR.

    Returns (nnt, trace) or (None, "CALCULATION_FAILED: reason").
    """
    try:
        import sympy as sp
        arr_val, err = _safe_float(arr, "ARR")
        if err:
            return None, f"CALCULATION_FAILED: {err}"
        if arr_val == 0:
            return None, "CALCULATION_FAILED: ARR=0, NNT is undefined (division by zero)"
        arr_sym = sp.Rational(arr_val).limit_denominator(10**9)
        nnt_sym = sp.Integer(1) / arr_sym
        result = float(nnt_sym)
        trace = f"NNT = 1 / ARR = 1 / {arr_val} = {result:.4f}"
        return result, trace
    except Exception as e:
        return None, f"CALCULATION_FAILED: sympy error: {e}"


def calculate_arr(
    risk_control: Any,
    risk_intervention: Any,
) -> tuple[float | None, str]:
    """ARR = risk_control - risk_intervention.

    Returns (arr, trace) or (None, "CALCULATION_FAILED: reason").
    """
    try:
        import sympy as sp
        rc_val, err = _safe_float(risk_control, "risk_control")
        if err:
            return None, f"CALCULATION_FAILED: {err}"
        ri_val, err = _safe_float(risk_intervention, "risk_intervention")
        if err:
            return None, f"CALCULATION_FAILED: {err}"
        rc_sym = sp.Rational(rc_val).limit_denominator(10**9)
        ri_sym = sp.Rational(ri_val).limit_denominator(10**9)
        arr_sym = rc_sym - ri_sym
        result = float(arr_sym)
        trace = (
            f"ARR = risk_control - risk_intervention = "
            f"{risk_control} - {risk_intervention} = {result:.4f}"
        )
        return result, trace
    except Exception as e:
        return None, f"CALCULATION_FAILED: sympy error: {e}"


def calculate_mean_from_sum(
    total: Any,
    n: Any,
) -> tuple[float | None, str]:
    """Mean = total / n.

    Returns (mean, trace) or (None, "CALCULATION_FAILED: reason").
    """
    try:
        import sympy as sp
        total_val, err = _safe_float(total, "total")
        if err:
            return None, f"CALCULATION_FAILED: {err}"
        n_val, err = _safe_float(n, "n")
        if err:
            return None, f"CALCULATION_FAILED: {err}"
        if n_val == 0:
            return None, "CALCULATION_FAILED: n=0, mean is undefined"
        if n_val < 0:
            return None, f"CALCULATION_FAILED: n={n_val} is negative, invalid"
        total_sym = sp.Rational(total_val).limit_denominator(10**9)
        n_sym = sp.Rational(n_val).limit_denominator(10**9)
        mean_sym = total_sym / n_sym
        result = float(mean_sym)
        trace = f"mean = total / n = {total_val} / {n_val} = {result:.4f}"
        return result, trace
    except Exception as e:
        return None, f"CALCULATION_FAILED: sympy error: {e}"


def calculate_pooled_mean(
    means: list[Any],
    ns: list[Any],
) -> tuple[float | None, str]:
    """Weighted pooled mean = sum(n_i * mean_i) / sum(n_i).

    Returns (pooled_mean, trace) or (None, "CALCULATION_FAILED: reason").
    """
    try:
        import sympy as sp
        if len(means) != len(ns):
            return None, "CALCULATION_FAILED: means and ns must have equal length"
        if not means:
            return None, "CALCULATION_FAILED: empty input lists"
        total_n = sp.Integer(0)
        weighted_sum = sp.Integer(0)
        for i, (m, n) in enumerate(zip(means, ns)):
            m_val, err = _safe_float(m, f"means[{i}]")
            if err:
                return None, f"CALCULATION_FAILED: {err}"
            n_val, err = _safe_float(n, f"ns[{i}]")
            if err:
                return None, f"CALCULATION_FAILED: {err}"
            if n_val <= 0:
                return None, f"CALCULATION_FAILED: ns[{i}]={n_val} must be positive"
            m_sym = sp.Rational(m_val).limit_denominator(10**9)
            n_sym = sp.Rational(n_val).limit_denominator(10**9)
            weighted_sum += n_sym * m_sym
            total_n += n_sym
        if total_n == 0:
            return None, "CALCULATION_FAILED: sum of n is zero"
        result = float(weighted_sum / total_n)
        trace = (
            f"pooled_mean = sum(n_i * mean_i) / sum(n_i) = "
            f"{float(weighted_sum):.4f} / {float(total_n):.0f} = {result:.4f}"
        )
        return result, trace
    except Exception as e:
        return None, f"CALCULATION_FAILED: sympy error: {e}"


def calculate_percent(
    numerator: Any,
    denominator: Any,
) -> tuple[float | None, str]:
    """Percentage = (numerator / denominator) * 100.

    Returns (pct, trace) or (None, "CALCULATION_FAILED: reason").
    """
    try:
        import sympy as sp
        num_val, err = _safe_float(numerator, "numerator")
        if err:
            return None, f"CALCULATION_FAILED: {err}"
        den_val, err = _safe_float(denominator, "denominator")
        if err:
            return None, f"CALCULATION_FAILED: {err}"
        if den_val == 0:
            return None, "CALCULATION_FAILED: denominator=0"
        num_sym = sp.Rational(num_val).limit_denominator(10**9)
        den_sym = sp.Rational(den_val).limit_denominator(10**9)
        pct_sym = (num_sym / den_sym) * 100
        result = float(pct_sym)
        trace = (
            f"percent = (numerator / denominator) * 100 = "
            f"({numerator} / {denominator}) * 100 = {result:.2f}%"
        )
        return result, trace
    except Exception as e:
        return None, f"CALCULATION_FAILED: sympy error: {e}"


def evaluate_formula(
    formula: str,
    variables: dict[str, Any],
) -> tuple[float | None, str]:
    """Evaluate a sympy formula string with provided variable bindings.

    Args:
        formula: sympy-compatible expression string e.g. "a / (a + b)"
        variables: dict of symbol → numeric value

    Returns:
        (result, trace) or (None, "CALCULATION_FAILED: reason")
    """
    try:
        import sympy as sp
        sym_vars = {}
        for name, val in variables.items():
            float_val, err = _safe_float(val, name)
            if err:
                return None, f"CALCULATION_FAILED: {err}"
            sym_vars[name] = sp.Rational(float_val).limit_denominator(10**9)
        expr = sp.sympify(formula)
        result_sym = expr.subs(sym_vars)
        if result_sym.is_number:
            result = float(result_sym)
            trace = (
                f"formula='{formula}' with {variables} → {result:.6f}"
            )
            return result, trace
        return None, f"CALCULATION_FAILED: expression did not reduce to a number: {result_sym}"
    except Exception as e:
        return None, f"CALCULATION_FAILED: sympy error: {e}"
