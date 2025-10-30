import logging
from typing import Optional

import pandas as pd
import os
import numpy as np
from datetime import datetime
from oauth2client import service_account
from googleapiclient.discovery import build
from time import time


def get_google_apis_with_service_account(log: logging.Logger):
    start_time = time()
    credentials = service_account.ServiceAccountCredentials.from_json_keyfile_name(
        "Secrets/service_account.json"
    )
    log.info(f"credentials loaded for service agent: time = {time() - start_time}")

    service_time = time()
    spreadsheet_service = build("sheets", "v4", credentials=credentials)
    drive_service = build("drive", "v3", credentials=credentials)

    log.info(f"spreads and drive services agents loaded: time = {time() - start_time}")
    return drive_service, spreadsheet_service


def get_name_key(
    spread_api, file_id: str = "1jRsjyeRFrRQg0OzqpHi-hnHPPOW7fq7BcuB74geunCo"
) -> pd.DataFrame:
    name_key_2d_list = (
        spread_api.spreadsheets()
        .values()
        .get(spreadsheetId=file_id, range="nameKey")
        .execute()["values"]
    )
    return pd.DataFrame(data=name_key_2d_list[1:], columns=name_key_2d_list[0])


def get_town_coordinates(
    spread_api, file_id: str = "1wk6gTJoGRpFZGYwi4FDfY-srXutqEfzaGO-7dWzlXwk"
) -> pd.DataFrame:
    town_coordinates_2d_list = (
        spread_api.spreadsheets()
        .values()
        .get(spreadsheetId=file_id, range="townCords")
        .execute()["values"]
    )
    town_coord_df = pd.DataFrame(
        data=town_coordinates_2d_list[1:], columns=town_coordinates_2d_list[0]
    )
    town_coord_df["Latitude"].apply(lambda x: float(x))
    town_coord_df["Latitude"] = town_coord_df["Latitude"].astype(float)
    town_coord_df["Longitude"].apply(lambda x: float(x))
    town_coord_df["Longitude"] = town_coord_df["Longitude"].astype(float)

    return town_coord_df


def get_county_coordinates(
    spread_api, file_id: str = "1_XoY0TPxS7wBbDBFjjJulULMmuD0VHRveHR2i9Xhiu8"
) -> pd.DataFrame:
    county_coordinates_2d_list = (
        spread_api.spreadsheets()
        .values()
        .get(spreadsheetId=file_id, range="countyCords")
        .execute()["values"]
    )
    county_coords_df = pd.DataFrame(
        data=county_coordinates_2d_list[1:], columns=county_coordinates_2d_list[0]
    )
    county_coords_df["Latitude"].apply(lambda x: float(x))
    county_coords_df["Latitude"] = county_coords_df["Latitude"].astype(float)
    county_coords_df["Longitude"].apply(lambda x: float(x))
    county_coords_df["Longitude"] = county_coords_df["Longitude"].astype(float)
    return county_coords_df


def get_datetime() -> str:
    return datetime.strftime(datetime.now(), "%Y-%m-%d-%H:%M")


def to_csv_file(path: str, name: str, df: pd.DataFrame) -> None:
    df.to_csv(f"{path}/{name}_{get_datetime()}.csv", index=False)


def to_csv_file_custom_time(path: str, name: str, df: pd.DataFrame, time: str) -> None:
    df.to_csv(f"{path}/{name}_{time}.csv", index=False)


def find_latest_file(file_type: str, path_to="cleanedData") -> pd.DataFrame:
    file_names = pd.DataFrame(
        data=os.listdir(f"{path_to}/{file_type}"), columns=["files"]
    )
    file_names[["type", "datetime"]] = file_names["files"].str.split("_", expand=True)
    file_names["datetime"] = pd.to_datetime(
        file_names["datetime"].apply(lambda x: x.replace(".csv", ""))
    )
    name = file_names.loc[
        file_names["datetime"] == file_names["datetime"].max(), "files"
    ].values[0]
    return pd.read_csv(f"{path_to}/{file_type}/{name}", low_memory=False)


def cleanup_str(string: str) -> str:
    if isinstance(string, str):
        return (
            string.lower()
            .strip()
            .replace("?", "")
            .replace("()", "")
            .replace("  ", " ")
            .replace("(pears)", "(pear)")
        )
    else:
        return string


def clean_events_strings(df: pd.DataFrame) -> pd.DataFrame:
    def _clean_event_string(event_label):
        event_label = "|" + event_label + "|"
        return (
            (
                event_label.lower()
                .replace(" agricul|", " agricultural society")
                .replace(" farme|", " farmers club")
                .replace(" associati", " associety")
                .replace(" meet|", " meeting")
                .replace(" soc|", " society")
                .replace(" s|", " society")
                .replace(" socie|", " society")
                .replace(" societ|", " society")
                .replace("'", "")
                .replace("co.", "county")
                .replace("soc.", "society")
                .replace("t. f.", "fair")
                .replace("ag.", "agricultural")
                .replace("w.", "west")
                .replace("e.", "east")
                .replace("s.", "south")
                .replace("n.", "north")
                .replace("no.", "north")
                .replace("so.", "south")
                .replace("agr.", "agricultural")
                .replace("hort.", "horticultural")
                .replace(" fair|", "")
                .replace("|", "")
                .strip()
            )
            if isinstance(event_label, str)
            else f"Error Event not string :{str(event_label)}"
        )

    df["Event"] = df["Event"].fillna("").map(_clean_event_string)
    return df.replace("", np.nan)
