import os
import pandas as pd
import requests


def qb_headers() -> dict:
    return {
        "QB-Realm-Hostname": os.getenv("QB_QBREALMHOSTNAME"),
        "User-Agent": os.getenv("QB_USERAGENT"),
        "Authorization": f"QB-USER-TOKEN {os.getenv('QB_AUTHORIZATION')}",
        "Content-Type": "application/json",
    }


def qb_balances_fields_join() -> list[str]:
    return [
        "year_fiscal",
        "agency_id",
        "fund_id",
        "program_id",
        "costcenter_id",
        "account_id",
        "project_id",
        "award_id",
    ]


def qb_balances_fields_query() -> list[str]:
    return qb_balances_fields_join() + [
        "qbid",
    ]


def qb_balances_fields_insert() -> list[str]:
    return qb_balances_fields_join() + [
        "account_3_id",
        "account_1_id",
        "budget_adjusted",
        "budget_spent",
        "budget_current",
    ]


def qb_balances_fields_all() -> list[str]:
    return qb_balances_fields_insert() + ["qbid"]


def qb_getfields_balances() -> dict:
    params = {"tableId": os.getenv("QB_TABLEID_BALANCES")}

    r = requests.get(
        url="https://api.quickbase.com/v1/fields", params=params, headers=qb_headers()
    )

    return {
        field["label"]: field["id"]
        for field in r.json()
        if field["label"] in qb_balances_fields_all()
    }


def qb_get_balances() -> pd.DataFrame:
    fields = qb_getfields_balances()
    field_ids = {v: k for k, v in fields.items()}
    field_ids_query = list(
        {v: k for k, v in fields.items() if k in qb_balances_fields_query()}
    )

    body = {
        "from": os.getenv("QB_TABLEID_BALANCES"),
        "select": field_ids_query,
        "where": f"{{{fields['year_fiscal']}.EX.{os.getenv('YEAR_FISCAL')}}}",
    }

    r = requests.post(
        url="https://api.quickbase.com/v1/records/query",
        json=body,
        headers=qb_headers(),
    )
    data = r.json()["data"]

    table = {}
    for field_id in field_ids_query:
        table[field_id] = [row[str(field_id)]["value"] for row in data]

    return pd.DataFrame(table).rename(columns=field_ids)


def qb_merge_balances(
    balances_qb: pd.DataFrame, balances_difs: pd.DataFrame
) -> pd.DataFrame:
    balances = balances_qb.merge(
        balances_difs, how="outer", on=qb_balances_fields_join()
    )

    print("Found", len(balances), "records total between Quickbase and DIFS")

    return balances


def balances_missing_qb(balances: pd.DataFrame) -> pd.DataFrame:
    missing_qb = (
        balances.loc[lambda df: df["qbid"].isna()]
        .drop(columns=["qbid"])
        .rename(columns=qb_getfields_balances())
    )

    print("Found", len(missing_qb), "records missing in Quickbase")

    return missing_qb


def balances_present_qb(balances: pd.DataFrame) -> pd.DataFrame:
    present_qb = balances.loc[lambda df: df["qbid"].notna()].rename(
        columns=qb_getfields_balances()
    )

    print("Found", len(present_qb), "records present in Quickbase")

    return present_qb


def qb_insert_balances(table: pd.DataFrame) -> None:
    data = [
        {k: {"value": v} for k, v in row.items()}
        for row in table.to_dict(orient="records")
    ]
    body = {"to": os.getenv("QB_TABLEID_BALANCES"), "data": data}

    r = requests.post(
        url="https://api.quickbase.com/v1/records", json=body, headers=qb_headers()
    )

    print("Inserted", len(r.json()["metadata"]["createdRecordIds"]), "records")


def qb_update_balances(table: pd.DataFrame) -> None:
    data = [
        {k: {"value": v} for k, v in row.items()}
        for row in table.to_dict(orient="records")
    ]
    body = {"to": os.getenv("QB_TABLEID_BALANCES"), "data": data, "mergeFieldId": 3}

    r = requests.post(
        url="https://api.quickbase.com/v1/records", json=body, headers=qb_headers()
    )

    print("Updated", len(r.json()["metadata"]["updatedRecordIds"]), "records")
    print("Left", len(r.json()["metadata"]["unchangedRecordIds"]), "records unchanged")
