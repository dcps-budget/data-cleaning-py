import os
import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()


def qb_headers():
    return {
        "QB-Realm-Hostname": os.getenv("QB_REALMHOSTNAME"),
        "User-Agent": os.getenv("QB_USERAGENT"),
        "Authorization": f"QB-USER-TOKEN {os.getenv('QB_AUTHORIZATION')}",
        "Content-Type": "application/json",
    }


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

    print(r.json())
