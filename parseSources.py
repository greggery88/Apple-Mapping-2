import sys
import pandas as pd
import numpy as np
import os
from datetime import datetime
import time
from gspread_pandas import Spread, Client
from utilities import *

pd.set_option("future.no_silent_downcasting", True)


def _separate_source(source_df: pd.DataFrame) -> pd.DataFrame:
    column_names = [
        "Publication",
        "Date",
        "Premiums",
        "Description",
        "Page #",
        "2nd Page #",
        "3rd Page #",
    ]
    for i in range(1, 5):
        split_source = source_df[f"Source: {str(i)}"].str.split("|", expand=True)
        split_source.columns = [
            f"Source: {str(i)} : " + column_names[n]
            for n in range(len(split_source.columns))
        ]
        source_df = source_df.join(split_source).drop(columns=f"Source: {str(i)}")
    return source_df


def _check_column_header(headers: list, log, county: str, sheet: str) -> None:
    """
    this function is used to debug the column headers. it checks each header against an
    expected header printing an error if needed.
    """
    if len(headers) == 0:
        log.error(
            f"No columns at all:>\n"
            f" - Info: there are not columns in this sheet\n"
            f" -  Loc: file = {county}, sheet = {sheet}\n"
            f" -  Fix: by-passing this sheet all data from {sheet} will not be recorded."
        )
        return None
    if "Year" not in headers:
        log.error(
            f"No column header in sheet matching: Year>\n"
            f" - Info: Current_headers are = {headers}\n"
            f" -  Loc: file = {county}, sheet = {sheet}\n"
            f" -  Fix: by-passing this sheet all data from {sheet} will not be recorded."
        )
    if "Event" not in headers:
        log.error(
            f"No column header in sheet matching: Event>.\n"
            f" - Info: Current_headers are = {headers}\n"
            f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
            f" -  Fix: non"
        )
    if "Location" not in headers:
        log.error(
            f"No column header in sheet matching: Location>.\n"
            f" - Info: Current_headers are = {headers}\n"
            f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
            f" -  Fix: non"
        )
    if "Source" not in headers:
        if "Sources" in headers:
            log.warning(
                f"No column header in sheet matching: Source>\n"
                f" - Info: Current_headers are = {headers}\n"
                f" -  Loc: Incorrect headers at: file = {county}, sheet = {sheet},\n"
                f" -  Fix: replacing (Source) column headers with (Sources).\n."
            )
        else:
            log.error(
                f"No column header in sheet matching: Source>\n"
                f" - Info: Current_headers are = {headers}\n"
                f" -  Loc: Incorrect headers at: file = {county}, sheet = {sheet},\n"
                f" -  Fix: if this fail replace with empty string.\n."
            )
    if "Source Date" not in headers:
        log.error(
            f"No column header in sheet matching: Source Date>.\n"
            f" - Info: Current_headers are = {headers}\n"
            f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
            f" -  Fix: non"
        )
    if "Page" not in headers:
        log.error(
            f"No column header in sheet matching: Page>.\n"
            f" - Info: Current_headers are = {headers}\n"
            f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
            f" -  Fix: non"
        )
    if "Notes" not in headers:
        log.error(
            f"No column header in sheet matching: Notes>.\n"
            f" - Info: Current_headers are = {headers}\n"
            f" -  Loc: located at: file = {county}, sheet = {sheet}\n"
            f" -  Fix: non"
        )
    if "Additional Notes" not in headers:
        pass


def _get_data_from_sheets(sheet_data, column, index, log):
    # column is the specific column head you want to excess.
    # Excel_sheet is the sheet data which you are searching through.
    # row is the int index of the give row.
    try:
        return sheet_data[column].iloc[index]
    except KeyError:
        if column == "Source":
            try:
                data = sheet_data["Sources"].iloc[index]
                return data
            except KeyError:
                log.error(
                    "Fix failed to replacing (Source) column header with (Sources) did not solve.\n"
                    " - replacing missing data with nan"
                )
                return np.nan
        elif column == "Location":
            return ""
        elif column == "Additional Notes":
            return ""

    except Exception as e:
        log.critical(f"Unknown error {e} fix pls :(")


def _clean_notes(notes_on_pre_des, filename, sheet, log):
    notes_clean = cleanup_str(notes_on_pre_des)
    if notes_clean == "premiums":
        return "premiums", ""
    elif notes_clean == "description":
        return "", "description"
    elif notes_clean == "des. & prem." or notes_clean == "des. & pre.":
        return "premiums", "description"
    elif notes_clean == "unknown":
        log.debug(
            f"Notes data is not a (premiums) or a (description): unknown>\n"
            f" - Info: instead notes is : ({notes_on_pre_des})\n"
            f" -  Loc: file = {filename} sheet = {sheet}\n"
            f" -  Fix: if this fail replace with empty string.\n."
        )
        return "", ""
    elif notes_clean != "":
        log.error(
            f"Notes data is not a (premiums) or a (description)>\n"
            f" - Info: instead notes is : ({notes_on_pre_des})\n"
            f" -  Loc: file = {filename} sheet = {sheet}\n"
            f" -  Fix: if this fail replace with empty string.\n."
        )
        return "", ""
    else:
        return "", ""


def _clean_date(date, filename, sheet, year, log):
    if str(date) != "":
        date = str(date)

        if "00:00:00" in date:
            date = (
                date.replace("00:00:00", "").replace("-", "/")
                # .replace("3754", "1754")
            )
        elif "unknown" in date.lower().strip():
            log.debug(
                f"Unable to get specific date: unknown>\n"
                f" - Info: given date is: ({date})\n"
                f" -  Loc: located at file = {filename}, sheet = {sheet}, year = {year}\n"
                f" -  Fix: if this fail replace Unknown with nan.\n"
            )
            return np.nan
        else:
            try:
                m, d, y = date.split("/")
                date = f"{y}/{m}/{d}"
            except ValueError:
                log.error(
                    f"Unable to unpack date>\n"
                    f" - Info: given date is: ({date})\n"
                    f" -  Loc: located at file = {filename}, sheet = {sheet}, year = {year}\n"
                    f" -  Fix: if this fail replace Unknown with nan.\n"
                )
                return np.nan
            except Exception as e:
                log.critical("Unexpected error: ", e)
                sys.exit(1)

        return pd.to_datetime(date, yearfirst=True)


def _clean_source_separate_page_number(
    page_numbers: datetime, county_name: str, sheet, year: int, log: logging.Logger
):
    if type(page_numbers) == np.int64 or type(page_numbers) == int:
        return [np.int64(page_numbers)]
    elif page_numbers == "":
        return [np.nan]
    else:
        if type(page_numbers) == datetime:
            page_numbers: str = (
                str(page_numbers)
                .replace("00", "")
                .replace(":", "")
                .replace("2025", "")
                .replace("-0", " ")
                .strip()
                .replace(" ", ",")
            )
        pn_clean = (
            page_numbers.lower()
            .strip()
            .replace("&", ",")
            .replace("  ", "")
            .replace(" ", "")
        )
        page_number_split = list(filter(None, pn_clean.split(",")))

        if "supplemental " in pn_clean:
            log.debug(
                f"Page number is not an integer is: (supplemental)>\n"
                f" - Info: page_numbers = {pn_clean}\n"
                f" - Loc: file = {county_name}, sheet = {sheet}, year = {year}\n"
                f" - Fix: return 0 (0 = supplemental)>\n"
            )
            page_number_split = [
                0 if pn == "supplement" else pn for pn in page_number_split
            ]
        if "unknown" in pn_clean:
            log.debug(
                f"Page number is not an integer is: (unknown)>\n"
                f" - Info: page_numbers = {pn_clean}\n"
                f" - Loc: file = {county_name}, sheet = {sheet}, year = {year}\n"
                f" - Fix: return nans>\n"
            )
            page_number_split = [
                np.nan if pn == "unknown" else pn for pn in page_number_split
            ]

        if (
            len(page_number_split) > 3
        ):  # checks to see if there are more than two_page numbers given
            log.critical(
                f"More than three page numbers>\n"
                f" - Info: page_numbers = {page_number_split}\n"
                f" - Loc: file = {county_name}, sheet = {sheet}, year = {year}\n"
                f" - Fix: replace with nans"
            )
            sys.exit(1)
        return page_number_split


def _get_data_from_sources(
    log: logging.Logger, gs_config: GSpreadConfig
) -> pd.DataFrame:
    time_start = time.time()
    t = 0
    # gathers the data from the Directory rawFairSources into a pandas dataFrame.

    # functions for processing the data as it's read from the Excel files.

    # list to story the data as is gathered from the Excel files
    rows = []

    client = Client(config=gs_config)
    spread_info = client.find_spreadsheet_files_in_folders("sources")["sources"]

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
        log.info(f"sources: starting file {t}/{len(spread_info)}, {county_name}...")

        spreadsheet = Spread(spread_id, config=gs_config)

        sheets = spreadsheet.sheets

        for sheet in sheets:

            sheet_name = sheet.title.strip().lower()
            spreadsheet.open_sheet(sheet)

            read_source_excel = spreadsheet.sheet_to_df().reset_index()

            col_heads = list(read_source_excel.columns)

            # runs the headers debug check.
            _check_column_header(
                headers=col_heads, log=log, county=county_name, sheet=sheet_name
            )

            for i in range(read_source_excel.shape[0]):

                try:
                    year: int = read_source_excel["Year"].iloc[i]
                except KeyError:
                    continue

                if year != " " and year != "":
                    # reads the data from the file.

                    location: str = _get_data_from_sheets(
                        read_source_excel, "Location", i, log=log
                    )
                    source_date = _get_data_from_sheets(
                        read_source_excel, "Source Date", i, log=log
                    )
                    source: str = _get_data_from_sheets(
                        read_source_excel, "Source", i, log=log
                    )
                    page_nums = _get_data_from_sheets(
                        read_source_excel, "Page", i, log=log
                    )
                    notes: str = _get_data_from_sheets(
                        read_source_excel, "Notes", i, log=log
                    )
                    additional_notes: str = _get_data_from_sheets(
                        read_source_excel, "Additional Notes", i, log=log
                    )

                    # uses previously define functions to clean the page # and the note(Premiums/description)
                    page_numbers_clean = _clean_source_separate_page_number(
                        page_nums,
                        county_name=county_name,
                        sheet=sheet_name,
                        year=year,
                        log=log,
                    )
                    premiums, description = _clean_notes(
                        notes, filename=county_name, sheet=sheet_name, log=log
                    )

                    # constructs a library and clean the rest of the data.
                    rows.append(
                        {
                            "County": county_name,
                            "Event": sheet_name,
                            "Year": int(year),
                            # "Old Event": cleanup_str(event),
                            "Location": cleanup_str(location),
                            "Publication": cleanup_str(source),
                            "Page #": page_numbers_clean[0],
                            "2nd Page #": (
                                page_numbers_clean[1]
                                if len(page_numbers_clean) == 2
                                else np.nan
                            ),
                            "3nd page #": (
                                page_numbers_clean[2]
                                if len(page_numbers_clean) == 3
                                else np.nan
                            ),
                            "Premiums": premiums,
                            "Description": cleanup_str(description),
                            "Additional Notes": cleanup_str(additional_notes),
                            "Date": _clean_date(
                                source_date,
                                filename=county_name,
                                sheet=sheet,
                                year=year,
                                log=log,
                            ),
                        }
                    )
        log.info(
            f"sources: {county_name} complete time taken = {time.time() - file_start_time}s"
        )
    log.info(
        f"gathering data from apple files complete! :)"
        f" - total to to get datetime total: {time.time() - time_start}s"
    )

    # make the data into a data frame.
    reformated_data = pd.DataFrame.from_records(rows)
    return reformated_data


def _check_for_duplicates(log, df) -> None:
    duplicated = df.loc(axis=0)[df.duplicated(keep="first")]
    if len(duplicated.index) > 0:
        log.error(
            f"Duplicated source Date Frame found:>\n"
            f" - Info: there is ({int(len(duplicated))}) sources duplicated\n"
            f"              these sources are\n"
            f" {duplicated}\n"
            f" -  Fix drop duplicates\n"
        )


def _add_cords(log, source_df):
    cords = pd.read_csv("longLatCords/townCords.csv")
    source_df = source_df.merge(cords, on=["Location"], how="left")

    source_df.replace("", np.nan, inplace=True)
    llc = pd.read_csv("longLatCords/countyCords.csv")
    source_df = source_df.merge(llc, on="County", suffixes=("", "_llc"), how="left")
    source_df.loc[source_df["Location"].isna(), ["Latitude"]] = source_df[
        "Latitude_llc"
    ]
    source_df.loc[source_df["Location"].isna(), ["Longitude"]] = source_df[
        "Longitude_llc"
    ]
    source_df.drop(["Latitude_llc", "Longitude_llc"], axis=1, inplace=True)

    if (
        source_df.loc[source_df["Location"].notna() & source_df["Longitude"].isna(),]
        .any()
        .sum()
        != 0
    ):
        log.error("Missing for for town name>/n")
        source_df.loc[
            source_df["Location"].notna() & source_df["Longitude"].isna()
        ].to_csv("DebugFile_missing_town_names.csv", index=False)
    return source_df


def _clean_location(value) -> str:
    if isinstance(value, str):
        return value.lower().replace("foxcroft", "dover-foxcroft").strip()
    else:
        return value


def _find_and_fix_source_duplicated_multiple_sheets(
    source_df: pd.DataFrame, log: logging.Logger
):

    duplicated = source_df.loc[
        (
            source_df.duplicated(
                subset=["Event", "Year", "Publication", "Date"],
                keep=False,
            )
        )
    ]
    if not duplicated.empty:
        log.error(
            f"Source in Multiple Sheets: duplicate in [Event, Year] not in [Sheet Names, Year]>\n"
            f" - Info: source in multiple sheet different source is check if the sheets are necessary to completely fix\n"
            f" - Data: {duplicated}\n"
            f" -  Fix: only keep the first data points\n"
        )

    new_source_df = source_df.loc[
        ~(
            source_df.duplicated(
                subset=["Event", "Year", "Publication", "Date"], keep="first"
            )
        )
    ]
    return new_source_df


def _clean_source_data(source_df):
    # combines the sources
    source_df["Source"] = source_df.loc(axis=1)[
        "Publication",
        "Date",
        "Premiums",
        "Description",
        "Page #",
        "2nd Page #",
        "3nd page #",
    ].apply(lambda x: "|".join(x.dropna().astype(str)), axis=1)

    # drop the old columns
    source_df.drop(
        columns=[
            "Publication",
            "Date",
            "Premiums",
            "Description",
            "Page #",
            "2nd Page #",
            "3nd page #",
        ],
        inplace=True,
    )

    # begin to clean the locations
    source_df["Location"] = source_df["Location"].apply(_clean_location)

    # rank the source i the
    source_df["rank"] = source_df.groupby(["Event", "Year", "County", "Location"])[
        "Source"
    ].rank()

    pivot = source_df.pivot(
        index=["Year", "Event", "County", "Location"],
        columns="rank",
        values="Source",
    )

    reformated_sources = source_df.merge(
        pivot, on=["Year", "Event", "County", "Location"], how="left"
    )

    reformated_sources.drop(columns=["Source", "rank"], inplace=True)

    reformated_sources.drop_duplicates(inplace=True)

    reformated_sources.rename(
        columns={
            1.0: "Source: 1",
            2.0: "Source: 2",
            3.0: "Source: 3",
            4.0: "Source: 4",
        },
        inplace=True,
    )

    return reformated_sources


""" this is the main function of this file. It is called by the main function and sets up processes the data."""


def parse_sources(
    log: logging.Logger, gs_config: GSpreadConfig, split_sources: bool = True
):
    log.info("Parsing Sources...")

    # gets the data from the sheets
    _source_data = _get_data_from_sources(log, gs_config)

    _check_for_duplicates(log, _source_data)
    _source_data.drop_duplicates(inplace=True)
    log.info("successfully removed duplicate data from sources :) !!")

    _source_data = _find_and_fix_source_duplicated_multiple_sheets(_source_data, log)
    log.info("removed duplicate data from the sources :) !!")

    _source_data = _clean_source_data(_source_data)

    _source_data = _add_cords(log, _source_data)
    log.info("successfully added cords data to the sources :) !!")

    _source_data = clean_events_strings(_source_data)
    log.info("successfully cleaned Events of sources :) !!")
    log.info(
        "successfully both parse and cleaned data from the source spreadsheets :) !!"
    )
    _check_for_duplicates(log, _source_data)
    _source_data.drop_duplicates(inplace=True)

    if split_sources:
        _source_data = _separate_source(_source_data)

    return _source_data


if __name__ == "__main__":
    sources_log = setup_logging("Sources Log")
    sources = parse_sources(sources_log, get_gs_config(), split_sources=False)
    to_csv_file(path="cleanedData/Sources", name="sources", df=sources)
