import numpy as np
import pandas as pd
import os
import time

from utilities import *

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

def get_data_from_sources():
    rows = []

    file_list = os.listdir(
        "rawFairSourcesData"
    )

    for county in file_list:

        sheet_names = pd.ExcelFile(
            f"rawFairSourcesData/{county}"
        ).sheet_names

        county_name = county.replace("Copy of Maine, ", "").replace(
            " County Sources.xlsx", ""
        )

        for fair in sheet_names:
            read_source_excel = pd.read_excel(
                f"rawFairSourcesData/{county}",
                sheet_name=fair,
                keep_default_na=False,
            )

            for i in range(read_source_excel.shape[0]):

                year: int = read_source_excel["Year"].iloc[i]

                if year != " " and year != "":

                    event: str = read_source_excel["Event"].iloc[i]
                    location: str = read_source_excel["Location"].iloc[i]
                    source_date = read_source_excel["Source Date"].iloc[i]
                    source: str = read_source_excel["Source"].iloc[i]
                    page_nums = read_source_excel["Page"].iloc[i]
                    notes: str = read_source_excel["Notes"].iloc[i]

                    def clean_source_separate_page_number(page_number):
                        second_page_num: int = ""
                        if type(page_number) == str:
                            if page_number != "":
                                page_number = page_number.replace("&", "").replace(",", " ").replace("  ", " ")
                                page_split = page_number.split()
                                if len(page_split) != 0:
                                    page_number = int(page_split[0].strip()) if page_split[0] != "Unknown" else ""
                                    if len(page_split) > 1:
                                        second_page_num = int(page_split[1].strip()) if page_split[
                                                                                            1].strip() != "Supplement" else "Supplement"
                        return page_number, second_page_num

                    page_num, second_page_num = clean_source_separate_page_number(page_nums)

                    def clean_notes(notes_on_pre_des):
                        notes_clean = cleanup_str(notes_on_pre_des)
                        premiums = ""
                        description = ""
                        if notes_clean == "premiums":
                            premiums = "premiums"
                        elif notes_clean == "description":
                            description = "description"
                        elif notes_clean == "des. & prem." or notes_clean == "des. & pre.":
                            premiums = "premiums"
                            description = "description"
                        elif notes_clean != "":
                            print(f"ERROR: not a premiums or a description instead notes is : ({notes_on_pre_des})")
                        return premiums, description
                    
                    premiums, description = clean_notes(notes)

                    def clean_date(date):
                        if str(date) != "":
                            date = str(date)
                            if "00:00:00" in date:
                                date = date.replace("00:00:00", "").replace("-", "/").replace("3754", "1754")
    
                            else:
                                m,d,y = date.split("/")
                                date = f"{y}/{m}/{d}"
                            return pd.to_datetime(date, yearfirst=True)
                        

                    rows.append({
                        "County": cleanup_str(county_name),
                         "Sheet Names": cleanup_str(fair),
                         "Year": int(year),
                         "Event": cleanup_str(event),
                         "Location": cleanup_str(location),
                         "Source": cleanup_str(source),
                         "Page #": page_num,
                         "2nd Page #": second_page_num,
                         "Premiums": premiums,
                         "Description": cleanup_str(description),
                         "Date": clean_date(source_date),
                    })


    reformated_data = pd.DataFrame.from_records(rows)
    return reformated_data

def add_debug_index(df):
    df = get_data_from_county_fairs()
    df["Debug Index IDs"] = df.index + 1
    return df

def add_source_ids(source_df):
    source_df['Source ID'] = source_df[source_df.columns[2:4]].apply(
        lambda x: ','.join(x.dropna().astype(str)),
        axis=1)
    return source_df




if __name__ == '__main__':
    source_data = get_data_from_sources()