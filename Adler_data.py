import pandas as pd
from utilities import (
    find_latest_file,
    get_datetime,
    to_csv_file_custom_time,
)


def make_adler_data(fair: str, df: pd.DataFrame, time: str) -> None:
    grouped_df = df.groupby("Event").get_group(fair).copy().reset_index()
    grouped_df.drop(columns=["Event"], inplace=True)
    to_csv_file_custom_time(df=grouped_df, path=f"AdlerData", name=fair, time=time)


def main() -> None:
    time = get_datetime()
    df = find_latest_file("Full")
    for Event in df["Event"].unique():
        make_adler_data(Event, df, time)


if __name__ == "__main__":
    main()
