import logging
import sys
import pandas as pd
import numpy as np
from utilities import setup_logging, find_latest_file, get_datetime, to_csv_file


def _fix_presumed_name(df: pd.DataFrame) -> pd.DataFrame:
    new_name = "Alt Presumed Names"
    nf = (
        df[["Use Name", "Presumed Name: Other"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"Presumed Name: Other": "Presumed Name: 1"})
        .copy()
    )
    groupby = (
        nf[["Use Name", "Presumed Name: 1"]]
        .drop_duplicates()
        .merge(
            nf,
            on=[
                "Use Name",
                "Presumed Name: 1",
            ],
            how="outer",
        )
    )
    groupby["rank"] = groupby.groupby("Use Name")["Presumed Name: 1"].rank()
    table = groupby.pivot(index="Use Name", columns="rank", values="Presumed Name: 1")

    table[new_name] = table.apply(
        lambda x: "|".join(x.dropna()).strip(), axis=1
    ).replace("", np.nan)

    return df.merge(
        (table.reset_index().loc[:, ["Use Name", new_name]].set_index("Use Name")),
        on="Use Name",
        how="outer",
    ).drop(columns=["Presumed Name: 1", "Presumed Name: Other"])


def _make_pivot_table_and_merge_on_use_name(
    df: pd.DataFrame, final_col_name: str, initial_col_name: str
) -> pd.DataFrame:
    nf = (
        df[["Use Name", initial_col_name]]
        .dropna(subset=[initial_col_name])
        .drop_duplicates()
    )
    nf["rank"] = (
        nf[["Use Name", initial_col_name]]
        .drop_duplicates()
        .groupby("Use Name")[initial_col_name]
        .rank()
    )
    table = nf.pivot(index="Use Name", columns="rank", values=initial_col_name)

    table[final_col_name] = table.apply(
        lambda x: "|".join(x.dropna()).strip(), axis=1
    ).replace("", np.nan)

    return df.merge(
        (
            table.reset_index()
            .loc[:, ["Use Name", final_col_name]]
            .set_index("Use Name")
        ),
        on="Use Name",
        how="outer",
    )


def _add_use_names(log: logging.Logger, df: pd.DataFrame) -> pd.DataFrame:
    log.info("Importing Name Key!")
    name_key = pd.read_csv("sourceKeys/NameKey.csv")

    # merge use name key with data!
    corrected_df = df.merge(
        name_key,
        on=["Given Name Clean", "IsApple"],
        how="left",
        suffixes=("_main_data", "_name_key"),
        indicator=False,
    )
    return corrected_df


def _autofill_use_name(log: logging.Logger, df: pd.DataFrame) -> pd.DataFrame:
    if df["Use Name"].isna().sum() != 0:
        # debug warning and make a file to story the information

        log.warning(
            "Auto filling use name: from 'Presumed Name: 1'>\n"
            " - Info: info data stored in file DebugFile_Autofilled_UseName:AltPresumedNameClean"
            "the file is located in <debugInfo\n"
            " -  this is not a massive problem just needs to be checked"
        )

        # write the information about the autofilled data point to a file called:  DebugFile_Autofilled_UseName:AltPresumedNameClean.
        df.loc[df["Use Name"].isna() & df["Presumed Name: 1"].notna()].to_csv(
            "debugInfo/DebugFile_Autofilled_UseName_AltPresumedNameClean.csv",
            index=False,
        )

        # autofill missing Use Name data from "Presumed Name: 1"
        df.loc[
            df["Use Name"].isna() & df["Presumed Name: 1"].notna(),
            "Debug Auto Filled Use Name From:",
        ] = "Presumed Name"
        df.loc[df["Use Name"].isna() & df["Presumed Name: 1"].notna(), "Use Name"] = df[
            "Presumed Name: 1"
        ]
        df.loc[
            df["Use Name"].isna() & df["Presumed Name: 1"].notna(),
            "Presumed Name: 1",
        ] = np.nan

    if df["Use Name"].isna().sum() != 0:
        # debug warning and make a file to story the information

        log.warning(
            "Auto filling use name: from Given Name Clean>\n"
            " - Info: info data stored in file DebugFile_Autofilled_UseName:GivenNameClean>\n"
            "the file is located in <debugInfo\n"
            " -  this is not a massive problem just needs to be checked\n"
        )

        # write the information about the autofilled data point to a file called:  DebugFile_Autofilled_UseName:AltPresumedNameClean.
        df[df["Use Name"].isna() & df["Given Name Clean"].notna()].to_csv(
            "debugInfo/DebugFile_Autofilled_UseName_GivenNameClean.csv", index=False
        )

        # autofill missing Use Name data from Given Name Clean.

        df.loc[
            df["Use Name"].isna() & df["Presumed Name: 1"].isna(),
            "Debug Auto Filled Use Name From:",
        ] = "Given Name Clean"
        df.loc[df["Use Name"].isna() & df["Presumed Name: 1"].isna(), "Use Name"] = df[
            "Given Name Clean"
        ]
        df.loc[
            df["Debug Auto Filled Use Name From:"].isna(),
            "Debug Auto Filled Use Name From:",
        ] = "Name Key"

    else:
        log.info("nothing to Auto Fill. Use Name complete!!")
    return df


def _fix_presumed_name_from_name_key_merge(
    log: logging.Logger, df: pd.DataFrame
) -> pd.DataFrame:
    # removes the duplicates between the use name and Presumed Name Clean_name_key and Presumed Name Clean_main_data.
    df.loc[
        df["Presumed Name Clean_name_key"] == df["Use Name"],
        "Presumed Name Clean_name_key",
    ] = np.nan

    df.loc[
        df["Presumed Name Clean_main_data"] == df["Use Name"],
        "Presumed Name Clean_main_data",
    ] = np.nan

    # fills the nans in Presumed Name Clean_name_key from Presumed Name Clean_main_data.
    df.loc[
        df["Presumed Name Clean_name_key"].isna(), "Presumed Name Clean_name_key"
    ] = df["Presumed Name Clean_main_data"]

    df.loc[df["Presumed Name Clean_name_key"].notna(), "Debug Presumed Name From:"] = (
        "Name Key"
    )
    df.loc[
        df["Presumed Name Clean_name_key"].isna() & df["Presumed Name Clean_main_data"],
        "Debug Presumed Name From:",
    ] = "Main Data"

    # removes duplicates between them
    df.loc[
        df["Presumed Name Clean_main_data"] == df["Presumed Name Clean_name_key"],
        "Presumed Name Clean_main_data",
    ] = np.nan

    if df["Presumed Name Clean_main_data"].notna().sum() == 0:
        df[["Presumed Name: 1", "Presumed Name: Other"]] = df[
            "Presumed Name Clean_name_key"
        ].str.split(pat=",", expand=True)
        df.drop(
            columns=["Presumed Name Clean_name_key", "Presumed Name Clean_main_data"],
            inplace=True,
        )
    else:
        df.rename(
            columns={
                "Presumed Name Clean_main_data": "Presumed Name: 1",
                "Presumed Name Clean_name_key": "Presumed Name: 2",
            },
            inplace=True,
        )
        df[df["Presumed Name: 1"].notna()].to_csv(
            "debugInfo/DebugFile_Multiple_AltNames.csv", index=False
        )

        log.critical(
            "Multiple Presumed Name: 1>\n"
            " - Info: multiple presumed names info in file:\n"
            "  DebugFile_Multiple_AltNames\n"
        )
        sys.exit(0)

    log.info("Finished fixing presumed names!")
    return df


def _separate_names(
    df: pd.DataFrame, names_col_name: str, new_cols_name: str
) -> pd.DataFrame:
    split_col = df[names_col_name].replace(np.nan, "").str.split(pat="|", expand=True)
    split_col.columns = [new_cols_name + str(i) for i in range(len(split_col.columns))]
    return df.join(split_col).drop(columns=[names_col_name])


def clean_apple_names(
    log: logging.Logger,
    apple_data: pd.DataFrame,
    debug_presumed_name_from: bool = False,
    debug_auto_filled_from: bool = False,
    separate_presumed_names: bool = False,
    separate_alt_names: bool = False,
    separate_given_names: bool = False,
) -> pd.DataFrame:
    apple_data.replace("", np.nan, inplace=True)
    apple_data = _add_use_names(log, apple_data)
    apple_data = _fix_presumed_name_from_name_key_merge(log, apple_data)
    apple_data = _autofill_use_name(log, apple_data)
    apple_data = _fix_presumed_name(apple_data)
    apple_data = _make_pivot_table_and_merge_on_use_name(
        apple_data, "Alt Names", "Alt Name Clean"
    ).drop(columns=["Alt Name Clean"])
    apple_data = _make_pivot_table_and_merge_on_use_name(
        apple_data, "Alt Given Names", "Given Name Clean"
    )

    # some logic for edit what info this give you
    if not debug_presumed_name_from:
        apple_data.drop(columns="Debug Presumed Name From:", inplace=True)
    if not debug_auto_filled_from:
        apple_data.drop(columns="Debug Auto Filled Use Name From:", inplace=True)
    if separate_alt_names:
        apple_data = _separate_names(apple_data, "Alt Names", "Alt Name Cln:")
    if separate_given_names:
        apple_data = _separate_names(apple_data, "Alt Given Names", "Given Name Cln:")
    if separate_presumed_names:
        apple_data = _separate_names(
            apple_data, "Alt Presumed Names", "Presumed Name Cln:"
        )

    return apple_data


if __name__ == "__main__":
    CLN_log = setup_logging("CLN log")
    data = find_latest_file("Apples")
    to_csv_file(
        df=clean_apple_names(CLN_log, data),
        path="cleanedData/AppleNamesCleaned",
        name="cleanApplesNames",
    )
    CLN_log.info("finished cleaning ApplesNames!")
