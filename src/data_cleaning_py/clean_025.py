import os
import pandas as pd

from datetime import datetime
from data_cleaning_py import paths, clean_209


def clean_025() -> pd.DataFrame:
    fy = "2025"

    path_025_in = os.path.join(paths.path_025(), "Raw", fy, "r025.xlsx")
    path_025_out = os.path.join(paths.path_025(), "Clean", fy, "r025.csv")

    r_025_raw = pd.read_excel(
        path_025_in,
        usecols="A,E,Q,W,K,Y,AA,AE,AJ,AF,AG,AH",
        dtype={
            "Agency": str,
            "Fund": str,
            "Program": str,
            "Cost Center": str,
            "Account": str,
            "Project": str,
            "Award": str,
            "Total Budget": float,
            "Available Budget": float,
            "Commitment": float,
            "Obligation": float,
            "Expenditure": float,
        },
        skiprows=18,
    )

    r_025_clean = (
        r_025_raw.rename(
            columns={
                "Agency": "agency_id",
                "Fund": "fund_id",
                "Program": "program_id",
                "Cost Center": "costcenter_id",
                "Account": "account_id",
                "Project": "project_id",
                "Award": "award_id",
                "Total Budget": "budget_adjusted",
                "Available Budget": "budget_current",
            }
        )
        .assign(
            spend=lambda df: df["Commitment"] + df["Obligation"] + df["Expenditure"]
        )
        .loc[
            lambda df: (df["agency_id"].isin(["GA0"]))
            | (df["project_id"].isin(clean_209.clean_209()["project_id"])),
            [
                "agency_id",
                "fund_id",
                "program_id",
                "costcenter_id",
                "account_id",
                "project_id",
                "award_id",
                "budget_adjusted",
                "budget_current",
                "spend",
            ],
        ]
    )

    r_025_clean.to_csv(path_025_out, index=False)

    print("R025:", "Finished", len(r_025_clean), "\trows at", datetime.now())

    return r_025_clean
