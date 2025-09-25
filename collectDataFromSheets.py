from fontTools.misc.arrayTools import pointInRect
from oauth2client import service_account
from googleapiclient.discovery import build
from ratelimit import limits, sleep_and_retry
from time import time
import pandas as pd

from utilities import to_csv_file

APPLE_FILES_FOLDER_ID = "1Clo7Sax3tSzm_xJGiVc4E2jMNDONaR8F"
SOURCE_FILES_FOLDER_ID = "1jQy_1sIVniSFhC2Zc4PfK-jS3N4AHcbi"

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _collect_data_from_google(folder_id):
    start_time = time()
    credentials = service_account.ServiceAccountCredentials.from_json_keyfile_name(
        "Secrets/service_account.json"
    )
    print("credentials loaded", time() - start_time)

    service_time = time()
    spreadsheet_service = build("sheets", "v4", credentials=credentials)
    drive_service = build("drive", "v3", credentials=credentials)
    print("services loaded", time() - service_time)

    apple_data_dict = _get_the_data_from_all_spreadsheets(
        drive_service, spreadsheet_service, folder_id
    )
    return apple_data_dict


def _get_the_data_from_all_spreadsheets(
    drive_api_service, sheet_api_service, folder_id: str
):
    apple_files_names_and_ids = _get_spreadsheet_names_and_ids(
        drive_api_service, folder_id=folder_id
    )
    full_data_dict = {}

    for spreadsheet_name_and_id in apple_files_names_and_ids:
        dict_of_spreadsheets = {}
        spreadsheet_name = spreadsheet_name_and_id["name"]
        spreadsheet_id = dict_of_spreadsheets["id"] = spreadsheet_name_and_id["id"]

        timey_wimey = time()
        dict_of_spreadsheets["data"] = _get_data_from_spreadsheet(
            sheet_api_service, spreadsheet_id
        )

        full_data_dict[spreadsheet_name] = dict_of_spreadsheets
        print("done with", spreadsheet_name, "!!!", time() - timey_wimey)
    return full_data_dict


# one call of drive API
def _get_spreadsheet_names_and_ids(
    drive_api_service, folder_id: str
) -> list[dict[str, str]]:
    spreadsheet_names_and_ids = (
        drive_api_service.files()
        .list(
            q=f"mimeType='application/vnd.google-apps.spreadsheet' and "
            f"trashed=false and "
            f"'{folder_id}' in parents",
            fields="files(id, name)",
        )
        .execute()
    )["files"]
    if spreadsheet_names_and_ids is None:
        raise Exception("unable to get spreadsheet names and ids!!")
    return spreadsheet_names_and_ids


# two calls of sheets API, one internal and one from get_sheets_names_from_spreadsheet
@sleep_and_retry
@limits(calls=25, period=60)
def _get_data_from_spreadsheet(sheet_api_service, spreadsheet_id: str) -> list[dict]:
    names_of_sheets = _get_sheets_names_from_spreadsheet(
        sheet_api_service, spreadsheet_id
    )
    return (
        sheet_api_service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=spreadsheet_id, ranges=names_of_sheets)
        .execute()
    )["valueRanges"]


# one call of sheets API
@sleep_and_retry
@limits(calls=20, period=60)
def _get_sheets_names_from_spreadsheet(
    sheet_api_service, spreadsheet_id: str
) -> list[str]:
    info_for_all_sheets = (
        sheet_api_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id)
        .execute()["sheets"]
    )
    return [sheet["properties"]["title"] for sheet in info_for_all_sheets]


def _make_df_of_all_spreads(raw_data_dict: dict) -> pd.DataFrame:
    _df_of_all_spreads = pd.DataFrame.from_records(
        raw_data_dict, index=None
    ).T.reset_index()
    _df_of_all_spreads.rename(columns={"index": "county name"}, inplace=True)
    _df_of_all_spreads["county name"] = _df_of_all_spreads.iloc(axis=1)[0].apply(
        lambda x: x.lower()
        .replace("county fairs", "")
        .replace("maine,", "")
        .replace("copy of ", "")
        .strip()
    )
    return _df_of_all_spreads


def _get_all_spreadsheet_data_into_pandas(_df_of_all_spreads: pd.DataFrame):
    def _applied_get_spreads_data_into_pandas(data_x):
        def _get_sheets_data_into_pandas(values_x):
            df = pd.DataFrame(values_x, index=None)
            df.columns = df.iloc[0]
            df.drop(index=0, inplace=True)

            # drop none columns
            if len(df.columns[df.columns.isna()]) > 0:
                print(f"dropped none columns: {df.columns[df.columns.isna()]}")
                df.drop(columns=df.columns[df.columns.isna()], inplace=True)
            return df

        df = pd.DataFrame.from_records(data_x)
        df.drop(columns="majorDimension", inplace=True)
        df["range"] = df["range"].str.split(pat="!", expand=True)[0]
        df.rename(columns={"range": "event name"}, inplace=True)
        df["event name"] = df["event name"].apply(
            lambda x: x.lower().replace("'", "").strip()
        )
        df["values"] = df["values"].apply(_get_sheets_data_into_pandas)
        return df

    _df_of_all_spreads["data"] = _df_of_all_spreads["data"].apply(
        _applied_get_spreads_data_into_pandas
    )
    return _df_of_all_spreads


def _add_county_event_name_to_df_of_all_spreads_data(
    _df_of_all_spreads: pd.DataFrame,
) -> pd.DataFrame:
    def _add_county_name_to_spreads_data(x):
        x["data"]["county name"] = x["county name"]

    def _add_county_and_name_to_sheets_data_alt(x):
        x["values"]["county name"] = x["county name"]
        x["values"]["event name"] = x["event name"]

    _df_of_all_spreads.apply(_add_county_name_to_spreads_data, axis=1)
    _df_of_all_spreads.apply(
        lambda x: x["data"].apply(_add_county_and_name_to_sheets_data_alt, axis=1),
        axis=1,
    )
    return _df_of_all_spreads


def _reformat_df_of_all_spreads_data(
    _df_of_all_spreads_data: pd.DataFrame,
) -> pd.DataFrame:
    list_of_sheets_data = []
    for i in range(len(_df_of_all_spreads_data)):
        list_of_sheets_data.append(
            pd.concat(_df_of_all_spreads_data["data"].iloc[i]["values"].tolist())
        )
    return pd.concat(list_of_sheets_data)


def get_data_from_google_sheets_into_one_large_data_frame(
    folder_id: str,
) -> pd.DataFrame:
    raw_data_dict = _collect_data_from_google(folder_id=folder_id)
    df_of_all_spreads = _make_df_of_all_spreads(raw_data_dict)
    df_of_all_spreads = _get_all_spreadsheet_data_into_pandas(df_of_all_spreads)
    df_of_all_spreads = _add_county_event_name_to_df_of_all_spreads_data(
        df_of_all_spreads
    )
    reformated_df_of_all_spreads = _reformat_df_of_all_spreads_data(df_of_all_spreads)
    return reformated_df_of_all_spreads.sort_index(axis=1)


if __name__ == "__main__":
    to_csv_file(
        get_data_from_google_sheets_into_one_large_data_frame(APPLE_FILES_FOLDER_ID),
    )
