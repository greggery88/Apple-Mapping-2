from utilities import setup_logging, get_datetime, find_latest_file
import pandas as pd
import numpy as np
from datetime import datetime


def _merge_indicators_to_as(x):
    if x == "left_only":
        return "from apple only"
    elif x == "right_only":
        return "from source only"
    else:
        return "from both"


def merge_sources_n_apples(log, apples, sources):
    df = apples.merge(
        sources,
        how="outer",
        suffixes=("_s", "_a"),
        indicator="Apples Sources Merge",
    )
    df["Apples Sources Merge"] = df["Apples Sources Merge"].apply(
        _merge_indicators_to_as
    )
    if len(df.loc[df["Apples Sources Merge"] == "from source only"]) != 0:
        log.error(
            "apple sources merge error: merging just sources>\n"
            " - Info: there is a sources whose event and year do not match\n"
            "the ones in the apples data this could bececause a spelling error of data not entered. find the different data here\n"
        )
        df.loc[df["Apples Sources Merge"] == "from source only"].to_csv(
            "debugInfo/DebugFile_S_A_merge_errors_overflow_from_sources.csv"
        )
        log.warning("Dropping data missing apples")

    if len(df.loc[df["Apples Sources Merge"] == "from apple only"]) != 0:
        log.error(
            "apple sources merge error: merging just apple>\n"
            " - Info: there is a sources whose event and year do not match\n"
            "the ones in the apples data this could bececause a spelling error of data not entered. find the different data here\n"
        )
        df.loc[df["Apples Sources Merge"] == "from apple only"].to_csv(
            "debugInfo/DebugFile_S_A_merge_errors_overflow_from_apple.csv"
        )
        log.warning("Dropping data missing sources")

    return df.loc[df["Apples Sources Merge"] == "from both"].drop(
        columns=["Apples Sources Merge"]
    )


if __name__ == "__main__":
    log = setup_logging("Merge Log")
    apples = find_latest_file("AppleNamesCleaned")
    sources = find_latest_file("Sources")
    merge_sources_n_apples(log, apples, sources).to_csv(
        f"cleanedData/Merged/merged_{get_datetime()}.csv", index=False
    )
