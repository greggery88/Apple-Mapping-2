import logging

import googleapiclient.errors
from ratelimit import limits, sleep_and_retry
import time
from time import time
import pandas as pd

import loggingUtil
from loggingUtil import log_formated
from utilities import to_csv_file, get_google_apis_with_service_account


APPLE_FILES_FOLDER_ID = "1Clo7Sax3tSzm_xJGiVc4E2jMNDONaR8F"
SOURCE_FILES_FOLDER_ID = "1jQy_1sIVniSFhC2Zc4PfK-jS3N4AHcbi"
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _collect_data_from_google(
    log: logging.Logger, folder_id: str, drive_service, spreadsheet_service
) -> dict:
    start_time_for_c_d_f_g = time()
    apple_data_dict = _get_the_data_from_all_spreadsheets(
        log=log,
        folder_id=folder_id,
        drive_api_service=drive_service,
        spread_api_service=spreadsheet_service,
    )
    log.info(
        f"data from all spreadsheets collected: time = {time() - start_time_for_c_d_f_g}"
    )
    return apple_data_dict


def _get_the_data_from_all_spreadsheets(
    log: logging.Logger, folder_id, drive_api_service, spread_api_service: str
):
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

    full_data_dict = {}

    for spreadsheet_name_and_id in spreadsheet_names_and_ids:
        start_time_of_get_data = time()

        dict_of_spreadsheets = {}

        spreadsheet_name = spreadsheet_name_and_id["name"]
        spreadsheet_id = dict_of_spreadsheets["id"] = spreadsheet_name_and_id["id"]

        dict_of_spreadsheets["data"] = _get_data_from_spreadsheet(
            spread_api_service, spreadsheet_id, log
        )

        full_data_dict[spreadsheet_name] = dict_of_spreadsheets

        log.info(
            f"finished getting data from {spreadsheet_name} ! : time = {time() - start_time_of_get_data}"
        )
    return full_data_dict


# two calls of sheets API, one internal and one from get_sheets_names_from_spreadsheet
@sleep_and_retry
@limits(calls=25, period=60)
def _get_data_from_spreadsheet(
    sheet_api_service, spreadsheet_id: str, log
) -> list[dict]:
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
        .replace("county", "")
        .replace("fairs", "")
        .replace("maine,", "")
        .replace("copy of ", "")
        .replace("sources", "")
        .strip()
    )
    return _df_of_all_spreads


def _get_all_spreadsheet_data_into_pandas(log, _df_of_all_spreads: pd.DataFrame):
    def _applied_get_spreads_data_into_pandas(data_x):
        def _get_sheets_data_into_pandas(values_x):
            df = pd.DataFrame(values_x, index=None)
            df.columns = df.iloc[0]
            df.drop(index=0, inplace=True)

            # drop none columns
            if len(df.columns[df.columns.isna()]) > 0:
                log_formated(
                    log.warning,
                    name="ColumnsContainingNoneValues",
                    info=f"The columns are: columns: {df.columns[df.columns.isna()]}",
                    fix="Dropping none columns",
                )

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
    log: logging.Logger,
    folder_id: str,
    drive_service,
    spreadsheet_service,
) -> pd.DataFrame:
    raw_data_dict = _collect_data_from_google(
        log=log,
        folder_id=folder_id,
        drive_service=drive_service,
        spreadsheet_service=spreadsheet_service,
    )
    df_of_all_spreads = _make_df_of_all_spreads(raw_data_dict)
    df_of_all_spreads = _get_all_spreadsheet_data_into_pandas(
        log=log, _df_of_all_spreads=df_of_all_spreads
    )
    df_of_all_spreads = _add_county_event_name_to_df_of_all_spreads_data(
        df_of_all_spreads
    )
    reformated_df_of_all_spreads = _reformat_df_of_all_spreads_data(df_of_all_spreads)
    return reformated_df_of_all_spreads.sort_index(axis=1)


def collect_data_from_google_sheets_main(log: logging.Logger):
    full_start_time = time()
    gcd_log = log.getChild("CollectDataFromGoogleLog")
    drive, spread = get_google_apis_with_service_account(log.getChild("GoogleApisLog"))
    start_time = time()
    raw_extracted_apple_data = get_data_from_google_sheets_into_one_large_data_frame(
        log=gcd_log,
        folder_id=APPLE_FILES_FOLDER_ID,
        drive_service=drive,
        spreadsheet_service=spread,
    )
    gcd_log.info(f"time to get apples: {time() - start_time}")

    start_time = time()
    raw_extracted_source_data = get_data_from_google_sheets_into_one_large_data_frame(
        folder_id=SOURCE_FILES_FOLDER_ID,
        log=gcd_log,
        drive_service=drive,
        spreadsheet_service=spread,
    )
    gcd_log.info(f"time to get sources: {time() - start_time}")
    gcd_log.info(f"full time to get the data from google: {time() - full_start_time}")

    return raw_extracted_apple_data, raw_extracted_source_data


if __name__ == "__main__":
    raw_apples, raw_sources = collect_data_from_google_sheets_main(
        loggingUtil.setup_logging("Sub_GCD")
    )

    to_csv_file(path="rawFiles/raw_apples", name="rawDataApples", df=raw_apples)

    to_csv_file(
        path="rawFiles/raw_sources",
        name="rawDataSources",
        df=raw_sources,
    )
