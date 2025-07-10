import os
import pandas as pd

from datetime import datetime


def clean_025():
    fy = "2025"

    username = str(os.environ.get("USERNAME"))
    path_r025 = os.path.join(
        "C:",
        os.sep,
        "Users",
        username,
        "OneDrive - District of Columbia Public Schools",
        "DCPS Budget - Data",
        "Budget",
        "R025",
    )

    path_r025_in = os.path.join(path_r025, "Raw", fy, "r025.xlsx")
    path_r025_out = os.path.join(path_r025, "Clean", fy, "r025.csv")

    r025_raw = pd.read_excel(
        path_r025_in,
        usecols="A,E,Q,W,K,Y,AA,AJ,AF,AG,AH",
        dtype={
            "Agency": str,
            "Fund": str,
            "Program": str,
            "Cost Center": str,
            "Account": str,
            "Project": str,
            "Award": str,
            "Available Budget": float,
            "Commitment": float,
            "Obligation": float,
            "Expenditure": float,
        },
        skiprows=18,
        nrows=38658,
    )

    r025_clean = (
        r025_raw.rename(
            columns={
                "Agency": "agency_id",
                "Fund": "fund_id",
                "Program": "program_id",
                "Cost Center": "costcenter_id",
                "Account": "account_id",
                "Project": "project_id",
                "Award": "award_id",
                "Available Budget": "available",
            }
        )
        .assign(spend=lambda x: x["Commitment"] + x["Obligation"] + x["Expenditure"])
        .loc[
            :,
            [
                "agency_id",
                "fund_id",
                "program_id",
                "costcenter_id",
                "account_id",
                "project_id",
                "award_id",
                "available",
                "spend",
            ],
        ]
    )

    r025_clean.to_csv(path_r025_out)

    print("Finished", "R025", "at", datetime.now())
