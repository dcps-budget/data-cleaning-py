from datetime import datetime
from data_cleaning_py import clean_025, quickbase


def main() -> None:
    print("Job is running!")

    fields_join = [
        "year_fiscal",
        "agency_id",
        "fund_id",
        "program_id",
        "costcenter_id",
        "account_id",
        "project_id",
        "award_id",
    ]

    fields = quickbase.qb_fields()

    balances_difs = clean_025.clean_025()
    balances_qb = quickbase.qb_balances()
    balances = balances_qb.merge(balances_difs, how="outer", on=fields_join)
    missing_from_qb = (
        balances.loc[lambda df: df["qbid"].isna()]
        .drop(columns=["qbid"])
        .rename(columns=fields)
        .to_dict(orient="records")
    )
    quickbase.qb_balances_insert(missing_from_qb)

    print("Finished", "overall job", "at", datetime.now())
