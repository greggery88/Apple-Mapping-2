import os
from sys import exception

import pandas as pd

pd.options.mode.copy_on_write = True

last_file = sorted(os.listdir("rawFiles"), reverse=True)[0]

apples_path = os.path.join("rawFiles", last_file, "Apples")
raw_data: pd.DataFrame = pd.DataFrame()

full_data_array = []

for (
    root,
    _,
    files,
) in os.walk(apples_path):
    if len(files) == 0:
        continue

    county_name = (
        root.split("/")[-1]
        .lower()
        .replace("copy of ", "")
        .replace("maine,", "")
        .replace("county fairs", "")
        .strip()
    )

    for file in files:
        event_name = file.replace(".csv", "").lower().strip()
        if (
            event_name == "varieties list"
            or event_name == "variety list"
            or event_name == "county totals"
        ):
            continue

        file_path = os.path.join(root, file)
        raw_data = pd.read_csv(file_path)

        # raw_data.drop(columns="Total", inplace=True)
        years_in_sheet = list(raw_data.columns[3:])
        list_of_dataframe_for_years = []
        for year in years_in_sheet:
            print(event_name, county_name, year)
            if not year.isdigit():
                print("not a year")
                print(year)
                continue
            df = raw_data.loc[raw_data[year].notna()]
            df.loc(axis=1)["Year"] = year

            list_of_dataframe_for_years.append(df)

        reformated_sheet_data = pd.concat(list_of_dataframe_for_years)

        reformated_sheet_data["Event"] = event_name
        reformated_sheet_data["County"] = county_name
        full_data_array.append(reformated_sheet_data)

df = pd.concat(full_data_array)
df.head()
