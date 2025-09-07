import logging
import pandas as pd
import os
import numpy as np
from datetime import datetime
from gspread_pandas import conf, Client, Spread
from os import path


def get_gs_config():
    service_account_file_path = "service_account.json"
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    return conf.get_config(
        path.abspath("service_account.json").replace("service_account.json", ""),
        file_name=service_account_file_path,
    )


def get_datetime() -> str:
    return datetime.strftime(datetime.now(), "%Y-%m-%d-%H:%M")


def to_csv_file(path: str, name: str, df: pd.DataFrame) -> None:
    df.to_csv(f"{path}/{name}_{get_datetime()}.csv", index=False)


def to_csv_file_custom_time(path: str, name: str, df: pd.DataFrame, time: str) -> None:
    df.to_csv(f"{path}/{name}_{time}.csv", index=False)


def find_latest_file(file_type: str) -> pd.DataFrame:
    file_names = pd.DataFrame(
        data=os.listdir(f"cleanedData/{file_type}"), columns=["files"]
    )
    file_names[["type", "datetime"]] = file_names["files"].str.split("_", expand=True)
    file_names["datetime"] = pd.to_datetime(
        file_names["datetime"].apply(lambda x: x.replace(".csv", ""))
    )
    name = file_names.loc[
        file_names["datetime"] == file_names["datetime"].max(), "files"
    ].values[0]
    return pd.read_csv(f"cleanedData/{file_type}/{name}")


def cleanup_str(string: str) -> str:
    return (
        string.lower()
        .strip()
        .replace("?", "")
        .replace("()", "")
        .replace("  ", " ")
        .replace("(pears)", "(pear)")
    )


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


def setup_logging(label: str) -> logging.Logger:
    # create logger with 'spam_application'
    logger = logging.getLogger(label)
    logger.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    ch.setFormatter(CustomFormatter())

    logger.addHandler(ch)
    return logger


class CustomFormatter(logging.Formatter):

    grey = "\x1b[37m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    green = "\x1b[32m"
    format = "%(levelname)s - %(name)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
