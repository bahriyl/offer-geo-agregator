import os
import importlib
import numpy as np
import pandas as pd
import pytest
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _prep_allocation_inputs(main_mod, df):
    dfw = df.copy()
    dfw.columns = [str(c).strip() for c in dfw.columns]
    E = pd.to_numeric(dfw.get("FTD qty", 0.0), errors="coerce").fillna(0.0)
    F = pd.to_numeric(dfw.get("Total spend", 0.0), errors="coerce").fillna(0.0)
    K = pd.to_numeric(dfw.get("Total Dep Amount", 0.0), errors="coerce").fillna(0.0)
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

    # First pass (no rows are green, so allocations remain zero)
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

    # Determine which index would receive the first leftover funds under the old ordering
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

    # Verify actual allocation gives the low-spend row a positive share
    _, _, alloc_result = main_mod.compute_allocation_max_yellow(df)
    assert alloc_result.at[low_idx] > 0.0
    assert alloc_result.at[high_idx] > 0.0
    assert alloc_result.at[low_idx] == pytest.approx(row_allowance.at[low_idx])


