import pandas as pd
import numpy as np
from utilities import (
    find_latest_file,
    get_datetime,
    to_csv_file,
    get_name_key,
)
from loggingUtil import setup_logging, log_formated_error_with_df

PER_SOURCE_COLUMNS = ["Source Name", "Page", "Notes", "Source Date", "Additional Notes"]


def _check_for_duplicates_sources(log, df) -> None:
    duplicated = df.loc(axis=0)[df.duplicated(keep="first")]
    if len(duplicated.index) > 0:
        log_formated_error_with_df(
            log=log,
            df=duplicated,
            error_name="DuplicatedSources",
            error_info=f"there is ({int(len(duplicated))}) sources duplicated",
            error_fix="Fix drop first duplicates source",
        )


def _clean_page_numbers(df):
    df.loc[df["Page"].notna(), "Page"] = df.loc[df["Page"].notna(), "Page"].apply(
        lambda x: (
            str(x).replace("&", "").replace(" ", "").replace(",", ";").strip().lower()
        )
    )

    return df


def _clean_year(df):
    df["Year"] = df["Year"].astype(np.int64)
    return df


def _clean_notes(df):
    df.loc[df["Notes"].notna(), "Notes"] = df["Notes"].str.strip().str.lower()
    df.loc[:, "Notes"] = df["Notes"].apply(
        lambda x: (
            x.replace("&", ";")
            .replace("des.", "description")
            .replace("prem.", "premium")
            .replace("pre.", "premium")
            .replace(" ", "")
            if x is not None
            else x
        )
    )
    return df


def _clean_dates(log, df):
    df["Source Date"] = df["Source Date"].replace("Unknown", np.nan)
    return df


def _clean_source_names(df):
    df.rename(columns={"Source": "Source Name"}, inplace=True)
    df.loc[:, "Source Name"] = df["Source Name"].apply(lambda x: x.lower().strip())
    return df


def _reconfigure_sources_to_one_per_event(df):
    df["fullSource"] = df[PER_SOURCE_COLUMNS].apply(
        lambda x: "|".join(x.replace(np.nan, "").astype(str)), axis=1
    )

    df.drop(columns=PER_SOURCE_COLUMNS, inplace=True)

    df["rank"] = df.groupby(["Event", "Year"])["fullSource"].rank()

    pivot = df.pivot(
        columns="rank", values="fullSource", index=["Event", "Year"]
    ).reset_index()

    rank_columns = pivot.columns[2:]

    for col in rank_columns:
        pivot[f"Source: {int(col)}"] = pivot[col]

    pivot.drop(columns=rank_columns, inplace=True)
    return (
        df.merge(pivot, on=["Event", "Year"], how="left")
        .drop(columns=["fullSource", "rank"])
        .drop_duplicates()
    )


def clean_sources(log, sources):
    _check_for_duplicates_sources(log, sources)
    sources.drop_duplicates(inplace=True)
    sources.replace("", np.nan, inplace=True)
    sources.drop(columns="Event", inplace=True)
    sources.rename(
        columns={"event name": "Event", "county name": "County"}, inplace=True
    )
    sources.dropna(subset=["Year"], inplace=True)
    sources = _clean_page_numbers(sources)
    sources = _clean_year(sources)
    sources = _clean_notes(sources)
    sources = _clean_dates(log, sources)

    sources = _clean_source_names(sources)
    sources = sources.replace("unknown", np.nan)

    sources = _reconfigure_sources_to_one_per_event(sources)
    return sources


if __name__ == "__main__":
    clean_sources_log = setup_logging("TestCleanSourcesLog")
    df_source = find_latest_file("Sources")
    to_csv_file(
        df=clean_sources(clean_sources_log, df_source),
        path="cleanedData/cleanSources",
        name="cleanSources",
    )
