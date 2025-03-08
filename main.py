import numpy as np
import pandas as pd
import os
import time

from utilities import *
from parseSources import *

def get_data_from_county_fairs():
    # for counts prints
    time_start = time.time()
    t = 0

    # rows for file construction
    rows = []

    # gets a list of the apple data files.
    file_list = os.listdir("rawAppleData")

    for county in file_list:


        apple_sheet_names = pd.ExcelFile(f"rawAppleData/{county}").sheet_names
        apple_sheet_names.remove('Varieties List')
        county_name = county.replace("Copy of Maine, ", "").replace(
            " County Fairs.xlsx", ""
        )

        # update counts and start a per-file timer and print.
        file_start_time = time.time()
        t += 1
        print(f"{t}/{len(file_list)}, {county_name}")

        for fair in apple_sheet_names:
            df = pd.read_excel(f"rawAppleData/{county}",sheet_name=fair,  keep_default_na=False)

            years = []

            for c in df.columns.values:
                if str(c).isdigit():
                    years.append(c)
            
                for year in years:
                    for i in range(df.shape[0]):
                        if df[year].iloc[i] != "":
                            try:
                                given_id: str = df["Variety"].iloc[i].replace("(Pears)", "(pear)").replace("(Pear)", "(pear)")
                                alt_id: str = df["Alt. Name Given"].iloc[i].replace("(Pears)", "(pear)").replace("(Pear)", "(pear)")
                                presumed_id: str = df["Presumed ID"].iloc[i].replace("(Pears)", "(pear)").replace("(Pear)", "(pear)")
    
                                is_apple = not "(pear)" in given_id or "(pear)" in presumed_id or "(pear)" in alt_id
    
                                given_id = given_id.replace("(pear)", "")
                                presumed_id = presumed_id.replace("(pear)", "")
                                alt_id = alt_id.replace("(pear)", "")
    
                                rows.append({
                                    "County": cleanup_str(county_name),
                                    "Fair": cleanup_str(fair),
                                    "Year": int(year),
                                    "IsApple": is_apple,
                                    "Given_ID": given_id,
                                    "Given_ID_Clean": cleanup_str(given_id),
                                    "Alt_ID": alt_id,
                                    "Alt_ID_Clean": cleanup_str(alt_id),
                                    "Presumed_ID": presumed_id,
                                    "Presumed_ID_Clean": cleanup_str(presumed_id),
                                })
                            
                            except Exception as e:
                                g = list(df.columns)
                                if g[2] != "Presumed ID":
                                    print(
                                        f"ERROR: Presumed ID is wrong it is ({g[2]}), the error is in {county} {fair}"
                                    )
                                if g[1] != "Alt. Name Given":
                                    print(
                                        f"ERROR: Alt. Name Given is wrong it is ({g[1]}), the error is in {county} {fair}"
                                    )
                                print(e)
                                
        print(f" time: {time.time() - file_start_time}s")
    print(f" time total: {time.time() - time_start}s")
    data = pd.DataFrame.from_records(rows)
    print("done reading files.")
    return data

def add_debug_index(df):
    df = get_data_from_county_fairs()
    df["Debug Index IDs"] = df.index + 1
    return df






if __name__ == '__main__':
    source_data = get_data_from_sources()