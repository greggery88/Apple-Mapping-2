import logging


from utilities import find_latest_file, to_csv_file
from loggingUtil import (
    setup_logging,
    log_formated_warning_with_df,
    log_formated_warning,
    log_formated_error_with_df,
)
from fuzzywuzzy import process
import pandas as pd
import numpy as np


EXPECT_COLUMNS_APPLES = ["Presumed ID", "Variety", "Notes", "Alt. Name Given"]
EXPECTED_COLUMNS_SOURCES = [
    "Additional Notes",
    "Event",
    "Location",
    "Notes",
    "Page",
    "Source",
    "Source Date",
    "Year",
    "county name",
    "event name",
]


def _check_for_missing_data_in_column(
    log: logging.Logger, _column: str, _df: pd.DataFrame
) -> bool:
    df_of_none_in_column = _df.loc[_df[_column].isna()].dropna(axis=1, how="all")
    if df_of_none_in_column.columns.to_list() == ["county name", "event name"]:
        return False
    if len(df_of_none_in_column.index) != 0:
        log_formated_warning_with_df(
            log=log,
            df=df_of_none_in_column,
            warning_name=f"MissingData-{_column}",
            warning_info=f"There is missing data in the {_column} column",
            warning_fix=f"This is not a problem just double check it",
        )

        return True
    return False


def _remove_miss_spelled_columns(
    log: logging.Logger, _expected_columns: list[str], _df: pd.DataFrame
) -> pd.DataFrame:
    for column in _expected_columns:
        column_matches = process.extract(column, _df.columns)
        column_matches = [match[0] for match in column_matches if match[1] > 90][1:]
        if len(column_matches) != 0:
            log_formated_warning(
                log=log,
                warning_name=f"{column}Misspellings".replace(" ", ""),
                warning_info=f"The misspelling variants are -> {column_matches}",
                warning_fix=f"Put the data from misspelled columns into {column}",
            )

            for match in column_matches:
                _df[column] = _df[column].fillna(_df[match])
                _df.drop(columns=match, inplace=True)
    return _df


def _reformat_apple_df(log: logging.Logger, apples_df: pd.DataFrame) -> pd.DataFrame:
    apple_df = find_latest_file(file_type="raw_apples", path_to="rawFiles")

    # resets nones to np.nans for parsing
    apple_df.replace({None: np.nan}, inplace=True)

    # remove the varieties lists and county total
    apple_df = apple_df.loc[
        ~apple_df["event name"].isin(
            ["varieties list", "county totals", "variety list"]
        )
    ]

    # get rid of the completely none columns
    apple_df.dropna(axis=1, how="all", inplace=True)

    # remove Notes and Totals this could be on a switch
    apple_df.drop(columns=["Notes", "Total"], inplace=True)

    # removes none values from Variety columns
    (
        apple_df.dropna(subset="Variety", inplace=True)
        if _check_for_missing_data_in_column(log, "Variety", apple_df)
        else apple_df
    )

    # removes misspelled columns
    apple_df = _remove_miss_spelled_columns(log, EXPECT_COLUMNS_APPLES, apple_df)

    # shimmy the data around to get a year column instead of individual columns for years
    years = apple_df.columns[apple_df.columns.str.isdigit()]
    year_dfs = []
    for year in years:
        n = apple_df.dropna(subset=year).drop(columns=years)
        n["year"] = year
        year_dfs.append(n)
    apple_df_reformated = pd.concat(year_dfs)
    apple_df_reformated["year"] = apple_df_reformated["year"].astype(int)

    return apple_df_reformated


def _reformat_source_df(log: logging.Logger, sources_df: pd.DataFrame) -> pd.DataFrame:
    sources_df.replace({None: np.nan}, inplace=True)
    sources_df.replace(" ", np.nan, inplace=True)
    sources_df = _remove_miss_spelled_columns(log, EXPECTED_COLUMNS_SOURCES, sources_df)

    (
        sources_df.dropna(subset="Source", inplace=True)
        if _check_for_missing_data_in_column(log, "Source", sources_df)
        else None
    )
    return sources_df


def _cleaning_apples(_log, _apples):
    _apples["Given Name Clean"] = _apples["Variety"].apply(_cleanup_str_if_string)
    _apples["Presumed Name Clean"] = _apples["Presumed ID"].apply(
        _cleanup_str_if_string
    )
    _apples["Alt Name Clean"] = _apples["Alt. Name Given"].apply(_cleanup_str_if_string)
    _apples["IsApple"] = _apples["Given Name Clean"].apply(lambda x: "(pear)" in x)
    _apples.rename(
        {
            "year": "Year",
            "Variety": "Raw Given Name",
            "county name": "County",
            "event name": "Event",
            "Presumed ID": "Raw Presumed Name",
            "Alt. Name Given": "Raw Alt Name",
        },
        axis=1,
        inplace=True,
    )
    return _apples


def _cleanup_str_if_string(string: str) -> str:
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


def _clean_events_strings(df: pd.DataFrame) -> pd.DataFrame:
    def _clean_event_string(event_label: str) -> str:
        if isinstance(event_label, str):
            event_label = "|" + event_label + "|"
            return (
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
        else:
            return f"Error Event not string :{str(event_label)}"
        return None

    df["Event"] = df["Event"].fillna("").map(_clean_event_string)
    return df.replace("", np.nan)


def reformate_data_and_clean_it(log, apples, sources):
    a_log = log.getChild("AppleLog")
    apples = _reformat_apple_df(a_log, apples)
    apples = _cleaning_apples(a_log, apples)
    apples = _clean_events_strings(apples)

    s_log = log.getChild("SourceLog")
    sources = _reformat_source_df(s_log, sources)
    return apples, sources


if __name__ == "__main__":
    r_log = setup_logging("TestReformatLog")
    apple = find_latest_file(file_type="raw_apples", path_to="rawFiles")
    source = find_latest_file("raw_sources", path_to="rawFiles")
    reformate_data_and_clean_it(r_log, apple, source)
