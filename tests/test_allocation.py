import importlib
import io
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("BOT_TOKEN", "0:TEST")

from main import (  # noqa: E402
    _build_threshold_table,
    _extract_targets,
    compute_allocation_max_yellow,
)


def test_allocation_continues_to_red_cap_when_budget_remains():
    df = pd.DataFrame(
        {
            "FTD qty": [10, 12],
            "Total spend": [200.0, 200.0],
            "Total Dep Amount": [150.0, 180.0],
            "Total+%": [500.0, 500.0],
            "CPA Target": [8.0, 8.0],
        },
        index=["row_a", "row_b"],
    )

    result_df, used_budget, alloc_vec = compute_allocation_max_yellow(df)
    assert not result_df.empty

    E = pd.to_numeric(df.get("FTD qty"), errors="coerce").fillna(0.0)
    K = pd.to_numeric(df.get("Total Dep Amount"), errors="coerce").fillna(0.0)
    targets, target_ints = _extract_targets(df)
    thresholds = _build_threshold_table(E, K, targets, target_ints)

    red_caps = thresholds["red_ceiling"].astype(float)
    yellow_caps = thresholds["yellow_soft_ceiling"].astype(float)

    assert float(df["Total spend"].sum()) > float(yellow_caps.sum()) + 1e-6

    np.testing.assert_allclose(alloc_vec.to_numpy(dtype=float), red_caps.to_numpy(dtype=float))
    assert used_budget == pytest.approx(float(red_caps.sum()))
    assert used_budget > float(yellow_caps.sum())


def _prep_allocation_inputs(main_mod, df):
    dfw = df.copy()
    dfw.columns = [str(c).strip() for c in dfw.columns]
    E = pd.to_numeric(dfw.get("FTD qty", 0.0), errors="coerce").fillna(0.0)
    F = main_mod._normalize_money(dfw.get("Total spend", pd.Series(0.0, index=dfw.index))).fillna(0.0)
    K = main_mod._normalize_money(dfw.get("Total Dep Amount", pd.Series(0.0, index=dfw.index))).fillna(0.0)
    T = pd.to_numeric(dfw.get("Total+%", 0.0), errors="coerce").fillna(0.0)
    targets, target_ints = main_mod._extract_targets(dfw)
    thresholds = main_mod._build_threshold_table(E, K, targets, target_ints)

    stop_before_red = thresholds["red_ceiling"].fillna(0.0)
    target_delta = (T - F).clip(lower=0.0)
    red_headroom = (stop_before_red - F).clip(lower=0.0)
    row_allowance = pd.Series(
        np.minimum(target_delta.to_numpy(), red_headroom.to_numpy()),
        index=dfw.index,
    )
    row_allowance = pd.to_numeric(row_allowance, errors="coerce").fillna(0.0)

    return {
        "E": E,
        "F": F,
        "K": K,
        "T": T,
        "targets": targets,
        "thresholds": thresholds,
        "row_allowance": row_allowance,
        "available_budget": float(target_delta.sum()),
    }


def test_low_spend_row_receives_leftover_before_high_spend():
    os.environ["BOT_TOKEN"] = "123:ABC"
    main_mod = importlib.reload(importlib.import_module("main"))

    df = pd.DataFrame(
        {
            "FTD qty": [40, 80],
            "Total spend": [50.0, 200.0],
            "Total Dep Amount": [20.0, 78.0],
            "Total+%": [80.0, 360.0],
            "CPA Target": [10.0, 20.0],
        }
    )

    inputs = _prep_allocation_inputs(main_mod, df)
    E = inputs["E"]
    F = inputs["F"]
    K = inputs["K"]
    targets = inputs["targets"]
    thresholds = inputs["thresholds"]
    row_allowance = inputs["row_allowance"]
    rem = inputs["available_budget"]

    spend_order = F.sort_values(ascending=True).index.tolist()
    alloc = pd.Series(0.0, index=df.index, dtype=float)

    F_now = F.copy()
    for idx in spend_order:
        if rem <= 1e-9:
            break
        allowance_left = float(row_allowance.at[idx] - alloc.at[idx])
        if allowance_left <= 1e-9:
            continue
        ei = float(E.at[idx])
        if ei <= 0:
            continue
        ki = float(K.at[idx])
        f_cur = float(F_now.at[idx])
        status_now = main_mod._classify_status(ei, f_cur, ki, float(targets.at[idx]))
        if status_now != "Green":
            continue
        target_yellow = main_mod._compute_make_yellow_target(ei, f_cur, ki, thresholds.loc[idx])
        if target_yellow is None:
            continue
        max_target = min(target_yellow, float(F.at[idx] + row_allowance.at[idx]))
        need = max_target - f_cur
        if need <= 1e-9:
            continue
        give = min(rem, need, allowance_left)
        if give <= 1e-9:
            continue
        alloc.at[idx] += give
        F_now.at[idx] += give
        rem -= give

    F_mid = F + alloc
    status_mid = pd.Series(
        [
            main_mod._classify_status(float(E.at[i]), float(F_mid.at[i]), float(K.at[i]), float(targets.at[i]))
            for i in df.index
        ],
        index=df.index,
    )
    yellow_limit = pd.Series(0.0, index=df.index, dtype=float)
    for idx in df.index:
        if status_mid.at[idx] != "Yellow":
            continue
        limit_val = main_mod._compute_yellow_limit(
            float(E.at[idx]),
            float(F_mid.at[idx]),
            float(K.at[idx]),
            thresholds.loc[idx],
        )
        limit_val = min(limit_val, float(F.at[idx] + row_allowance.at[idx]))
        yellow_limit.at[idx] = max(limit_val, float(F_mid.at[idx]))
    headroom = (yellow_limit - F_mid).clip(lower=0.0)

    low_idx = F.idxmin()
    high_idx = F.idxmax()
    assert low_idx != high_idx

    def _first_recipient(order):
        for idx in order:
            if headroom.at[idx] <= 1e-9:
                continue
            allowance_left = float(row_allowance.at[idx] - alloc.at[idx])
            if allowance_left <= 1e-9:
                continue
            return idx
        return None

    first_new = _first_recipient(spend_order)
    first_old = _first_recipient(headroom.sort_values(ascending=False).index.tolist())

    assert first_new == low_idx
    assert first_old == high_idx
    assert first_new != first_old

    _, _, alloc_result = main_mod.compute_allocation_max_yellow(df)
    assert alloc_result.at[low_idx] > 0.0
    assert alloc_result.at[high_idx] > 0.0
    alloc_delta = alloc_result - F
    assert alloc_delta.at[low_idx] == pytest.approx(row_allowance.at[low_idx])


def test_allocation_parses_currency_strings_with_non_standard_formats():
    os.environ["BOT_TOKEN"] = "789:XYZ"
    main_mod = importlib.reload(importlib.import_module("main"))

    df = pd.DataFrame(
        {
            "FTD qty": [20, 15, 12],
            "Total spend": ["1\u00a0234,56", "1,234.56", "$500"],
            "Total Dep Amount": ["800", "650", "$300"],
            "Total+%": [2500, 2200, 1200],
            "CPA Target": [8.0, 9.0, 10.0],
        },
        index=["nbsp_comma", "comma_dot", "currency"],
    )

    result_df, used_budget, alloc_vec = main_mod.compute_allocation_max_yellow(df)

    expected = np.array([1234.56, 1234.56, 500.0])
    parsed = main_mod._normalize_money(df.get("Total spend")).to_numpy(dtype=float)
    np.testing.assert_allclose(parsed, expected, rtol=1e-9)

    assert used_budget > 0.0
    assert used_budget == pytest.approx(float(alloc_vec.sum()))
    assert (alloc_vec.to_numpy(dtype=float) > 0.0).all()
    assert alloc_vec.index.tolist() == df.index.tolist()


def test_classify_status_uses_red_cutoff_from_below_and_excel_rule_matches():
    os.environ["BOT_TOKEN"] = "147:STATUS"
    main_mod = importlib.reload(importlib.import_module("main"))

    e = 10.0
    target = 8.0
    red_cutoff = target * main_mod.RED_MULT

    cpa_below = red_cutoff - 1e-4
    f_below = (cpa_below * e) / 1.3
    high_deposit = 50.0
    assert main_mod._classify_status(e, f_below, high_deposit, target) == "Red"

    cpa_above = red_cutoff + 1e-4
    f_above = (cpa_above * e) / 1.3
    status_above = main_mod._classify_status(e, f_above, high_deposit, target)
    assert status_above == "Grey"

    df = pd.DataFrame(
        {
            "Subid": ["s_below", "s_above"],
            "Offer ID": ["o1", "o2"],
            "Назва Офферу": ["Offer", "Offer"],
            "ГЕО": ["G1", "G2"],
            "FTD qty": [e, e],
            "Total spend": [0.0, 0.0],
            "Total Dep Amount": [1.3 * f_below * 0.5, 1.3 * f_above * 0.5],
        }
    )

    bio = io.BytesIO()
    new_spend = pd.Series([f_below, f_above], index=df.index)
    main_mod.write_result_like_excel_with_new_spend(
        bio,
        df,
        new_spend,
        overwrite_total_spend=True,
    )
    bio.seek(0)
    wb = load_workbook(bio)
    ws = wb["Result"]

    red_rule_formulae = []
    for rules in ws.conditional_formatting._cf_rules.values():
        for rule in rules:
            if getattr(rule, "type", None) != "expression":
                continue
            formulas = getattr(rule, "formula", [])
            if isinstance(formulas, str):
                formulas = [formulas]
            for formula in formulas:
                if "$H2<$I2" in formula:
                    red_rule_formulae.append(formula)

    assert any(f"$H2<$I2*{main_mod.RED_MULT:.1f}" in f for f in red_rule_formulae)


def test_read_result_allocation_table_handles_formula_total_plus_percent(tmp_path):
    os.environ["BOT_TOKEN"] = "456:FORM"
    main_mod = importlib.reload(importlib.import_module("main"))

    source_df = pd.DataFrame(
        {
            "FTD qty": [25],
            "Total spend": [180.0],
            "Total Dep Amount": [120.0],
            "Total+%": ["=B2*1.3"],
            "CPA Target": [9.0],
        }
    )

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        source_df.to_excel(writer, sheet_name="Result", index=False)
    file_bytes = bio.getvalue()

    parsed = main_mod.read_result_allocation_table(file_bytes, "result.xlsx")
    assert pytest.approx(parsed.at[0, "Total spend"], rel=1e-9) == 180.0
    assert parsed.at[0, "Total+%"] > parsed.at[0, "Total spend"]

    E = pd.to_numeric(parsed.get("FTD qty"), errors="coerce").fillna(0.0)
    F = parsed["Total spend"].astype(float)
    K = parsed["Total Dep Amount"].astype(float)
    targets, target_ints = main_mod._extract_targets(parsed)
    thresholds = main_mod._build_threshold_table(E, K, targets, target_ints)
    stop_before_red = thresholds["red_ceiling"].fillna(0.0)
    row_allowance = pd.Series(
        np.minimum(parsed["Total+%"].to_numpy(dtype=float), stop_before_red.to_numpy(dtype=float)),
        index=parsed.index,
    ).clip(lower=0.0)

    assert row_allowance.iloc[0] > 0.0

    result_df, used_budget, alloc_vec = main_mod.compute_allocation_max_yellow(parsed)
    assert used_budget > 0.0
    assert alloc_vec.iloc[0] > 0.0
    assert result_df.loc[parsed.index[0], "Allocated extra"] >= 0.0


def test_yellow_formula_written_to_excel_matches_helper():
    os.environ["BOT_TOKEN"] = "456:FORMULA"
    main_mod = importlib.reload(importlib.import_module("main"))

    df = pd.DataFrame(
        {
            "Subid": ["s1"],
            "Offer ID": ["o1"],
            "Назва Офферу": ["Offer"],
            "ГЕО": ["Geo"],
            "FTD qty": [5],
            "Total spend": [100.0],
            "Total Dep Amount": [50.0],
        }
    )

    bio = io.BytesIO()
    main_mod.write_result_like_excel_with_new_spend(
        bio,
        df,
        pd.Series([0.0]),
        overwrite_total_spend=True,
    )
    bio.seek(0)
    wb = load_workbook(bio)
    ws = wb["Result"]

    expected_formula = main_mod._build_yellow_formula()
    formulas = []
    for rules in ws.conditional_formatting._cf_rules.values():
        for rule in rules:
            if getattr(rule, "type", None) != "expression":
                continue
            formula_field = getattr(rule, "formula", None)
            if not formula_field:
                continue
            if isinstance(formula_field, (list, tuple)):
                formulas.extend(formula_field)
            else:
                formulas.append(formula_field)

    assert expected_formula in formulas
