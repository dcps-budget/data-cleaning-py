import os
import pandas as pd

from data_cleaning_py import paths


def clean_209(year_fiscal: str) -> pd.DataFrame:
    path_209_in = os.path.join(paths.path_209(), "Raw", year_fiscal, "r209.xlsx")
    path_209_out = os.path.join(paths.path_209(), "Clean", year_fiscal, "r209.csv")

    r_209_raw = pd.read_excel(
        path_209_in,
        usecols="A,S",
        dtype={
            "Agency": str,
            "Project": str,
        },
        skiprows=16,
    )

    r_209_clean = (
        r_209_raw.rename(
            columns={
                "Agency": "agency_id",
                "Project": "project_id",
            }
        )
        .loc[
            lambda df: df["agency_id"].isin(["GD0"]),
            [
                "agency_id",
                "project_id",
            ],
        ]
        .drop_duplicates()
    )

    r_209_clean.to_csv(path_209_out, index=False)

    print("Processed", len(r_209_clean), "records from", "R209")

    return r_209_clean
