from datetime import datetime

from data_cleaning_py import clean_025


def main() -> None:
    clean_025.clean_025()

    print("Finished", "overall job", "at", datetime.now())
