import os
import pandas as pd

from data_cleaning_py import paths


def clean_025(year_fiscal: str, projects: pd.DataFrame) -> pd.DataFrame:
    path_025_in = os.path.join(paths.path_025(), "Raw", year_fiscal, "r_025.xls")
    path_025_out = os.path.join(paths.path_025(), "Clean", year_fiscal, "r_025.csv")

    r_025_raw = pd.read_excel(
        path_025_in,
        usecols="A,E,G,I,K,Q,W,Y,AA,AE,AF,AG,AH,AJ",
        dtype={
            "Agency": str,
            "Fund": str,
            "Program": str,
            "Cost Center": str,
            "Account Category (Parent Level 3)": str,
            "Account Group (Parent Level 1)": str,
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
                "Account Category (Parent Level 3)": "account_3_id",
                "Account Group (Parent Level 1)": "account_1_id",
                "Account": "account_id",
                "Project": "project_id",
                "Award": "award_id",
                "Total Budget": "budget",
                "Available Budget": "available",
            }
        )
        .assign(
            year_fiscal=int(year_fiscal),
            spent=lambda df: df["Commitment"] + df["Obligation"] + df["Expenditure"],
        )
        .loc[
            lambda df: (df["agency_id"].isin(["GA0"]))
            | (df["project_id"].isin(projects["project_id"])),
            [
                "year_fiscal",
                "agency_id",
                "fund_id",
                "program_id",
                "costcenter_id",
                "account_3_id",
                "account_1_id",
                "account_id",
                "project_id",
                "award_id",
                "budget",
                "spent",
                "available",
            ],
        ]
    )

    r_025_clean.to_csv(path_025_out, index=False)

    print("Processed", len(r_025_clean), "records from", "R025")

    return r_025_clean
