from datetime import datetime
from dotenv import load_dotenv

from data_cleaning_py import clean_025, clean_209, quickbase, util


load_dotenv()
config = util.get_config(util.envs_expected())


def main() -> None:
    print("Job is running!")

    year_fiscal = config["YEAR_FISCAL"]
    projects = clean_209.clean_209(year_fiscal)

    balances_qb = quickbase.get_balances(year_fiscal, config)
    balances_difs = clean_025.clean_025(year_fiscal, projects)
    balances = quickbase.balances_merge(balances_qb, balances_difs)

    quickbase.balances_missing_difs(balances)

    missing_qb = quickbase.balances_missing_qb(balances)
    if len(missing_qb) > 0:
        quickbase.insert_balances(missing_qb, config)

    present_qb = quickbase.balances_present_qb(balances)
    if len(present_qb) > 0:
        quickbase.update_balances(present_qb, config)

    print("Finished overall job at", datetime.now())
