import pandas as pd
import requests


def qb_headers(config: dict[str, str]) -> dict[str, str]:
    return {
        "QB-Realm-Hostname": config["QB_QBREALMHOSTNAME"],
        "Authorization": f"QB-USER-TOKEN {config['QB_AUTHORIZATION']}",
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


def qb_getfields_balances(config: dict[str, str]) -> dict[str, int]:
    params = {"tableId": config["QB_TABLEID_BALANCES"]}
    r = requests.get(
        url=f"{config['QB_API']}/fields",
        params=params,
        headers=qb_headers(config),
    )

    return {
        field["label"]: field["id"]
        for field in r.json()
        if field["label"] in qb_balances_fields_all()
    }


def qb_get_balances(year_fiscal: str, config: dict[str, str]) -> pd.DataFrame:
    fields = qb_getfields_balances(config)
    field_ids = {v: k for k, v in fields.items()}
    field_ids_query = list(
        {v: k for k, v in fields.items() if k in qb_balances_fields_query()}
    )

    body = {
        "from": config["QB_TABLEID_BALANCES"],
        "select": field_ids_query,
        "where": f"{{{fields['year_fiscal']}.EX.{year_fiscal}}}",
    }
    r = requests.post(
        url=f"{config['QB_API']}/records/query",
        json=body,
        headers=qb_headers(config),
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
    missing_qb = balances.loc[lambda df: df["qbid"].isna()].drop(columns=["qbid"])

    print("Found", len(missing_qb), "records missing in Quickbase")

    return missing_qb


def balances_present_qb(balances: pd.DataFrame) -> pd.DataFrame:
    present_qb = balances.loc[lambda df: df["qbid"].notna()]

    print("Found", len(present_qb), "records present in Quickbase")

    return present_qb


def qb_insert_balances(table: pd.DataFrame, config: dict[str, str]) -> None:
    fields = qb_getfields_balances(config)
    data = [
        {k: {"value": v} for k, v in row.items()}
        for row in table.rename(columns=fields).to_dict(orient="records")
    ]

    body = {"to": config["QB_TABLEID_BALANCES"], "data": data}
    r = requests.post(
        url=f"{config['QB_API']}/records",
        json=body,
        headers=qb_headers(config),
    )

    print("Inserted", len(r.json()["metadata"]["createdRecordIds"]), "records")


def qb_update_balances(table: pd.DataFrame, config: dict[str, str]) -> None:
    fields = qb_getfields_balances(config)
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
        headers=qb_headers(config),
    )

    print("Updated", len(r.json()["metadata"]["updatedRecordIds"]), "records")
    print("Left", len(r.json()["metadata"]["unchangedRecordIds"]), "records unchanged")
