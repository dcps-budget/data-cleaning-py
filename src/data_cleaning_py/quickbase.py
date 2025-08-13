import pandas as pd
import requests


def balances_fields_join() -> list[str]:
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


def balances_fields_query() -> list[str]:
    return balances_fields_join() + [
        "qbid",
    ]


def balances_fields_insert() -> list[str]:
    return balances_fields_join() + [
        "account_3_id",
        "account_1_id",
        "budget",
        "spent",
        "available",
    ]


def balances_fields_all() -> list[str]:
    return balances_fields_insert() + ["qbid"]


def balances_merge(
    balances_qb: pd.DataFrame, balances_difs: pd.DataFrame
) -> pd.DataFrame:
    balances = balances_qb.merge(balances_difs, how="outer", on=balances_fields_join())

    print("Found", len(balances), "records total between Quickbase and DIFS")

    return balances


def balances_missing_difs(balances: pd.DataFrame) -> pd.DataFrame:
    missing_difs = balances.loc[lambda df: df["available"].isna()]

    print("Found", len(missing_difs), "records missing in DIFS")

    return missing_difs


def balances_missing_qb(balances: pd.DataFrame) -> pd.DataFrame:
    missing_qb = balances.loc[lambda df: df["qbid"].isna()]

    print("Found", len(missing_qb), "records missing in Quickbase")

    return missing_qb.drop(columns=["qbid"])


def balances_present_qb(balances: pd.DataFrame) -> pd.DataFrame:
    present_qb = balances.loc[lambda df: df["qbid"].notna()]

    print("Found", len(present_qb), "records present in Quickbase")

    return present_qb


def headers(config: dict[str, str]) -> dict[str, str]:
    return {
        "QB-Realm-Hostname": config["QB_QBREALMHOSTNAME"],
        "Authorization": f"QB-USER-TOKEN {config['QB_AUTHORIZATION']}",
        "Content-Type": "application/json",
    }


def getfields_balances(config: dict[str, str]) -> dict[str, int]:
    params = {"tableId": config["QB_TABLEID_BALANCES"]}
    r = requests.get(
        url=f"{config['QB_API']}/fields",
        params=params,
        headers=headers(config),
    )

    return {
        field["label"]: field["id"]
        for field in r.json()
        if field["label"] in balances_fields_all()
    }


def get_balances(year_fiscal: str, config: dict[str, str]) -> pd.DataFrame:
    fields = getfields_balances(config)
    field_ids = {v: k for k, v in fields.items()}
    field_ids_query = list(
        {v: k for k, v in fields.items() if k in balances_fields_query()}
    )

    body = {
        "from": config["QB_TABLEID_BALANCES"],
        "select": field_ids_query,
        "where": f"{{{fields['year_fiscal']}.EX.{year_fiscal}}}",
    }
    r = requests.post(
        url=f"{config['QB_API']}/records/query",
        json=body,
        headers=headers(config),
    )
    data = r.json()["data"]

    table = {}
    for field_id in field_ids_query:
        table[field_id] = [row[str(field_id)]["value"] for row in data]

    return pd.DataFrame(table).rename(columns=field_ids)


def insert_balances(table: pd.DataFrame, config: dict[str, str]) -> None:
    fields = getfields_balances(config)
    data = [
        {k: {"value": v} for k, v in row.items()}
        for row in table.rename(columns=fields).to_dict(orient="records")
    ]

    body = {"to": config["QB_TABLEID_BALANCES"], "data": data}
    r = requests.post(
        url=f"{config['QB_API']}/records",
        json=body,
        headers=headers(config),
    )

    print("Inserted", len(r.json()["metadata"]["createdRecordIds"]), "records")


def update_balances(table: pd.DataFrame, config: dict[str, str]) -> None:
    fields = getfields_balances(config)
    data = [
        {k: {"value": v} for k, v in row.items()}
        for row in table.rename(columns=fields).to_dict(orient="records")
    ]

    body = {
        "to": config["QB_TABLEID_BALANCES"],
        "data": data,
        "mergeFieldId": fields["qbid"],
    }
    r = requests.post(
        url=f"{config['QB_API']}/records",
        json=body,
        headers=headers(config),
    )

    print("Updated", len(r.json()["metadata"]["updatedRecordIds"]), "records")
    print("Left", len(r.json()["metadata"]["unchangedRecordIds"]), "records unchanged")
