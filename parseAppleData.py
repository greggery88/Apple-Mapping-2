import logging
import sys
import pandas as pd
import time
from gspread_pandas import Spread, Client
from utilities import (
    cleanup_str,
    setup_logging,
    clean_events_strings,
    to_csv_file, get_gs_config,
)
from fuzzywuzzy import process

pd.set_option("future.no_silent_downcasting", True)


def _get_data_from_sheets(
    sheet_data: pd.DataFrame, column: str, index: int, log: logging.Logger
) -> str:
    # column is the specific column head you want to excess.
    # Excel_sheet is the sheet data which you are searching through.
    # row is the int index of the give row.
    try:
        return sheet_data[column].iloc[index]
    except KeyError:
        try:
            if column == "Presumed ID":
                most_likely_presumed_id = process.extractOne(
                    query="Presumed ID", choices=list(sheet_data.columns)
                )
                if most_likely_presumed_id[1] < 90:
                    log.warning(
                        f"bad match: {most_likely_presumed_id[1]}>"
                        f"- Fix: failed to replacing (Presumed ID) did not solve.>\n"
                        " - replacing missing data with nan"
                    )
                    return ""

                data = sheet_data[most_likely_presumed_id[0]].iloc[index]

                return data
            elif column == "Alt. Name Given":
                most_likely_presumed_id = process.extractOne(
                    query="Alt. Name Given", choices=list(sheet_data.columns)
                )
                if most_likely_presumed_id[1] < 90:
                    log.warning(
                        f"bad match: {most_likely_presumed_id[1]}>"
                        f"- Fix: failed to replacing (Alt. Name Given) did not solve.>\n"
                        " - replacing missing data with nan"
                    )
                data = sheet_data[most_likely_presumed_id[0]].iloc[index]
                return data

            elif column == "Variety":
                most_likely_presumed_id = process.extractOne(
                    query="Variety", choices=list(sheet_data.columns)
                )
                if most_likely_presumed_id[1] < 90:
                    log.warning(
                        f"bad match: {most_likely_presumed_id[1]}>"
                        f"- Fix: failed to replacing (Variety) did not solve.>\n"
                        " - replacing missing data with nan"
                    )
                data = sheet_data[most_likely_presumed_id[0]].iloc[index]
                return data
        except Exception as e:
            log.critical(f"unknown problem gathering data with best fit exception: {e}")
            sys.exit(1)


def _check_column_header_for_apples(
    headers: list,
    log,
    county: str,
    sheet: str,
) -> None:
    if len(headers) == 0:
        log.error(
            f"No columns at all:>\n"
            f" - Info: there are not columns in this sheet\n"
            f" -  Loc: file = {county}, sheet = {sheet}\n"
            f" -  Fix: by-passing this sheet all data from {sheet} will not be recorded."
        )
        return None
    if "Variety" not in headers:
        most_likely_presumed_id = process.extractOne(query="Variety", choices=headers)
        if most_likely_presumed_id[1] > 90:
            log.warning(
                f"No column header in sheet matching: Variety>.\n"
                f" - Info: Current_headers are = {headers}\n"
                f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
                f" -  Fix: replace with closest match: {most_likely_presumed_id}\n."
            )
        else:
            log.error(
                f"No column header in sheet matching and not close matches: Variety>.\n"
                f" - Info: Current_headers are = {headers}\n"
                f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
                f" -  Fix: replace data with blank string: {most_likely_presumed_id}\n."
            )
    if "Alt. Name Given" not in headers:
        most_likely_presumed_id = process.extractOne(
            query="Alt. Name Given", choices=headers
        )
        if most_likely_presumed_id[1] > 90:
            log.warning(
                f"No column header in sheet matching: Alt. Name Given>.\n"
                f" - Info: Current_headers are = {headers}\n"
                f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
                f" -  Fix: replace with closest match: {most_likely_presumed_id}\n."
            )
        else:

            log.error(
                f"No column header in sheet matching and not close matches: Alt. Given Name>.\n"
                f" - Info: Current_headers are = {headers}\n"
                f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
                f" -  Fix: replace data with blank string: {most_likely_presumed_id}\n."
            )
    if "Presumed ID" not in headers:
        most_likely_presumed_id = process.extractOne(
            query="Presumed ID", choices=headers
        )
        if most_likely_presumed_id[1] > 90:
            log.warning(
                f"No column header in sheet matching: Presumed ID>.\n"
                f" - Info: Current_headers are = {headers}\n"
                f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
                f" -  Fix: replace with closest match: {most_likely_presumed_id}\n."
            )
        else:

            log.error(
                f"No column header in sheet matching and not close matches: Presumed ID>.\n"
                f" - Info: Current_headers are = {headers}\n"
                f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
                f" -  Fix: replace data with blank string: {most_likely_presumed_id}\n."
            )


def _get_data_from_county_fairs(
    log: logging.Logger, gs_config
) -> pd.DataFrame:
    # for counts prints
    time_start = time.time()
    t = 0

    # rows for file construction
    rows = []

    client = Client(config=gs_config)
    spread_info = client.find_spreadsheet_files_in_folders("ApplesFiles")["ApplesFiles"]

    for info_dict in spread_info:
        spread_id = info_dict["id"]
        county_name = (
            info_dict["name"]
            .replace("Copy of", "")
            .replace("Maine,", "")
            .replace("County Fairs", "")
            .strip()
            .lower()
        )
        # update counts and start a per-file timer and print.
        file_start_time = time.time()
        t += 1
        log.info(f"apples: starting file {t}/{len(spread_info)}, {county_name}...")

        spreadsheet = Spread(spread_id, config=gs_config)

        sheets = spreadsheet.sheets

        for sheet in sheets:

            sheet_name = sheet.title.strip().lower()

            if sheet_name != "varieties list":

                spreadsheet.open_sheet(sheet)

                df = spreadsheet.sheet_to_df().reset_index()

                years = []

                for c in df.columns.values:
                    if str(c).isdigit():
                        years.append(c)
                col_headers = list(df.columns)
                _check_column_header_for_apples(
                    log=log, county=county_name, sheet=sheet_name, headers=col_headers
                )

                for year in years:
                    for i in range(df.shape[0]):
                        if not pd.isnull(df[year].iloc[i]):
                            if df[year].iloc[i] != "":
                                given_name: str = _get_data_from_sheets(
                                    sheet_data=df, column="Variety", index=i, log=log
                                )
                                alt_name: str = _get_data_from_sheets(
                                    sheet_data=df,
                                    column="Alt. Name Given",
                                    index=i,
                                    log=log,
                                )
                                presumed_id: str = _get_data_from_sheets(
                                    sheet_data=df,
                                    column="Presumed ID",
                                    index=i,
                                    log=log,
                                )

                                is_apple = not "(pear)" in cleanup_str(given_name)

                                rows.append(
                                    {
                                        "County": county_name,
                                        "Event": cleanup_str(sheet_name),
                                        "Year": int(year),
                                        "IsApple": is_apple,
                                        "Given Name": given_name,
                                        "Given Name Clean": cleanup_str(given_name),
                                        "Old Alt Name": alt_name,
                                        "Alt Name Clean": cleanup_str(alt_name),
                                        "Old Presumed Name": presumed_id,
                                        "Presumed Name Clean": cleanup_str(presumed_id),
                                    }
                                )

        log.info(
            f"apple: {county_name} complete time taken = {time.time() - file_start_time}s"
        )
    log.info(
        f"gathering data from apple files complete! :)"
        f" - total to to get datetime total: {time.time() - time_start}s"
    )
    data = pd.DataFrame.from_records(rows)
    return data


def _check_for_duplicates(log: logging.Logger, df: pd.DataFrame) -> None:
    duplicated = df.loc(axis=0)[df.duplicated(keep="first")]
    if len(duplicated.index) > 0:
        log.error(
            f"Duplicated source Date Frame found:>\n"
            f" - Info: there is ({int(len(duplicated))}) apple duplicated\n"
            f"              these sources are\n"
            f" {duplicated}\n"
            f" -  Fix drop duplicates\n"
        )


def parse_apple(log: logging.Logger, gs_config) -> pd.DataFrame:
    log.info("Parsing apple data...")
    apples_df = _get_data_from_county_fairs(log=log, gs_config=gs_config)

    _check_for_duplicates(log=log, df=apples_df)
    apples_df.drop_duplicates(keep="first", inplace=True)
    log.info("successfully dropped duplicate apple data!")

    apples_df = clean_events_strings(apples_df)
    log.info("successfully gathered and cleaned apple data!")
    return apples_df


if __name__ == "__main__":  # this is only for debugging this file
    apple_log = setup_logging("Apples log")
    apples = parse_apple(apple_log, get_gs_config())
    to_csv_file(path="cleanedData/Apples", name="apples", df=apples)
