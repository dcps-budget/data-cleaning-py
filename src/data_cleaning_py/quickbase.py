import os
import pandas as pd
import requests


def qb_headers():
    return {
        "QB-Realm-Hostname": os.getenv("QB_REALMHOSTNAME"),
        "User-Agent": os.getenv("QB_USERAGENT"),
        "Authorization": f"QB-USER-TOKEN {os.getenv('QB_AUTHORIZATION')}",
        "Content-Type": "application/json",
    }


def qb_balances_fields_join():
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


def qb_fields():
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

    fields_insert = fields_join + [
        "account_3_id",
        "account_1_id",
        "budget_adjusted",
        "budget_spent",
        "budget_current",
    ]
    fields_all = fields_insert + ["qbid"]

    params = {"tableId": "bu9duyip8"}

    r = requests.get(
        url="https://api.quickbase.com/v1/fields", params=params, headers=qb_headers()
    )

    return {
        field["label"]: field["id"]
        for field in r.json()
        if field["label"] in fields_all
    }


def qb_balances():
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

    fields_query = fields_join + ["qbid"]

    fields_insert = fields_join + [
        "account_3_id",
        "account_1_id",
        "budget_adjusted",
        "budget_spent",
        "budget_current",
    ]
    fields_all = fields_insert + ["qbid"]

    params = {"tableId": "bu9duyip8"}

    r = requests.get(
        url="https://api.quickbase.com/v1/fields", params=params, headers=qb_headers()
    )

    fields = {
        field["label"]: field["id"]
        for field in r.json()
        if field["label"] in fields_all
    }

    field_ids = {v: k for k, v in fields.items()}

    field_ids_query = list({v: k for k, v in fields.items() if k in fields_query})

    body = {"from": "bu9duyip8", "select": field_ids_query}

    r = requests.post(
        url="https://api.quickbase.com/v1/records/query",
        json=body,
        headers=qb_headers(),
    )

    balances = r.json()["data"]

    balances_normalized = {}
    for field_id in field_ids_query:
        balances_normalized[field_id] = []
        for balance in balances:
            balances_normalized[field_id].append(balance[str(field_id)]["value"])

    balances_normalized = pd.DataFrame(balances_normalized).rename(columns=field_ids)

    return balances_normalized


def qb_balances_insert(balances):
    data = []
    for row in balances:
        data.append({k: {"value": v} for k, v in row.items()})

    body = {"to": "bu9duyip8", "data": data}

    r = requests.post(
        url="https://api.quickbase.com/v1/records", json=body, headers=qb_headers()
    )

    print("Inserted", len(r.json()["metadata"]["createdRecordIds"]), "records")


def qb_balances_merge(balances_qb, balances_difs):
    balances = balances_qb.merge(
        balances_difs, how="outer", on=qb_balances_fields_join()
    )

    print("Found", len(balances), "records total between Quickbase and DIFS")

    return balances


def balances_missing_qb(balances):
    missing_qb = (
        balances.loc[lambda df: df["qbid"].isna()]
        .drop(columns=["qbid"])
        .rename(columns=qb_fields())
        .to_dict(orient="records")
    )

    print("Found", len(missing_qb), "records missing in Quickbase")

    return missing_qb


def balances_present_qb(balances):
    present_qb = (
        balances.loc[lambda df: df["qbid"].notna()]
        .rename(columns=qb_fields())
        .to_dict(orient="records")
    )

    print("Found", len(present_qb), "records present in Quickbase")

    return present_qb


def qb_balances_update(balances):
    data = []
    for row in balances:
        data.append({k: {"value": v} for k, v in row.items()})

    body = {"to": "bu9duyip8", "data": data, "mergeFieldId": 3}

    r = requests.post(
        url="https://api.quickbase.com/v1/records", json=body, headers=qb_headers()
    )

    print("Updated", len(r.json()["metadata"]["updatedRecordIds"]), "records")
    print("Left", len(r.json()["metadata"]["unchangedRecordIds"]), "records unchanged")
