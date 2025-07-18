from datetime import datetime
from data_cleaning_py import clean_025, quickbase
from dotenv import load_dotenv


load_dotenv()


def main() -> None:
    print("Job is running!")

    balances_qb = quickbase.qb_balances()
    balances_difs = clean_025.clean_025()

    balances = quickbase.qb_balances_merge(
        balances_qb=balances_qb, balances_difs=balances_difs
    )

    missing_qb = quickbase.balances_missing_qb(balances=balances)
    if len(missing_qb) > 0:
        quickbase.qb_balances_insert(missing_qb)

    present_qb = quickbase.balances_present_qb(balances=balances)
    if len(present_qb) > 0:
        quickbase.qb_balances_update(present_qb)

    print("Finished overall job at", datetime.now())
