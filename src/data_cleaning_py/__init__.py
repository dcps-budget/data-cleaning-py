from datetime import datetime
from data_cleaning_py import clean_025, quickbase, util
from dotenv import load_dotenv


load_dotenv()
config = util.get_config(util.envs_expected())


def main() -> None:
    print("Job is running!")

    year_fiscal = config["YEAR_FISCAL"]

    balances_qb = quickbase.qb_get_balances(year_fiscal, config)
    balances_difs = clean_025.clean_025(year_fiscal)

    balances = quickbase.qb_merge_balances(balances_qb, balances_difs)

    missing_qb = quickbase.balances_missing_qb(balances)
    if len(missing_qb) > 0:
        quickbase.qb_insert_balances(missing_qb, config)

    present_qb = quickbase.balances_present_qb(balances)
    if len(present_qb) > 0:
        quickbase.qb_update_balances(present_qb, config)

    print("Finished overall job at", datetime.now())
