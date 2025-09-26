import os

import numpy as np
import pandas as pd
import pytest

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Ensure the bot token is present before importing the bot module
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

    # Переконуємося, що після доведення до жовтого ще є запас бюджету
    assert float(df["Total spend"].sum()) > float(yellow_caps.sum()) + 1e-6

    # Алгоритм повинен довести рядки до red_ceiling, коли бюджет усе ще доступний
    np.testing.assert_allclose(alloc_vec.to_numpy(dtype=float), red_caps.to_numpy(dtype=float))
    assert used_budget == pytest.approx(float(red_caps.sum()))
    assert used_budget > float(yellow_caps.sum())
