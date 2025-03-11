import pandas as pd
import os
import time
from utilities import *

def _get_data_from_sources(keep_sheet_names=False):
    # gathers the data from the Directory rawFairSources into a pandas dataFrame.




    # functions for processing the data as it's read from the Excel files.
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

    def clean_date(date):
        if str(date) != "":
            date = str(date)
            if "00:00:00" in date:
                date = date.replace("00:00:00", "").replace("-", "/").replace("3754", "1754")

            else:
                m, d, y = date.split("/")
                date = f"{y}/{m}/{d}"
            return pd.to_datetime(date, yearfirst=True)

    # list to story the data as is gathered from the Excel files
    rows = []

    # set up for main loop to loop over all file in the source data directory
    file_list = os.listdir("rawFairSourcesData")
    for county in file_list:

        # gets the sheet name from the Excel file.
        sheet_names = pd.ExcelFile(f"rawFairSourcesData/{county}").sheet_names

        # gets and processes the names of the county's
        county_name = county.replace("Copy of Maine, ", "").replace(
            " County Sources.xlsx", "")

        # loops over the sheet names in the Excel file
        for fair in sheet_names:


            read_source_excel = pd.read_excel(
                f"rawFairSourcesData/{county}",
                sheet_name=fair,
                keep_default_na=False,
            )

            for i in range(read_source_excel.shape[0]):

                year: int = read_source_excel["Year"].iloc[i]
                if year != " " and year != "":
                    # reads the data from the file.
                    event: str = read_source_excel["Event"].iloc[i]
                    location: str = read_source_excel["Location"].iloc[i]
                    source_date = read_source_excel["Source Date"].iloc[i]
                    source: str = read_source_excel["Source"].iloc[i]
                    page_nums = read_source_excel["Page"].iloc[i]
                    notes: str = read_source_excel["Notes"].iloc[i]

                    # uses previously define functions to clean the page # and the note(Premiums/description)
                    page_num, second_page_num = clean_source_separate_page_number(page_nums)
                    premiums, description = clean_notes(notes)

                    # constructs a library and clean the rest of the data.
                    rows.append({
                        "County": cleanup_str(county_name),
                        "Sheet Names": cleanup_str(fair),
                        "Year": int(year),
                        "Event": cleanup_str(event),
                        "Location": cleanup_str(location),
                        "Publication": cleanup_str(source),
                        "Page #": page_num,
                        "2nd Page #": second_page_num,
                        "Premiums": premiums,
                        "Description": cleanup_str(description),
                        "Date": clean_date(source_date),
                    })
    # make the data into a data frame.
    reformated_data = pd.DataFrame.from_records(rows)

    # remove the Sheet Name column because....
    if not keep_sheet_names:
        reformated_data = reformated_data.drop(columns="Sheet Names").drop_duplicates()

    return reformated_data

def _add_cords(source_df):
    cords = pd.read_csv("longLatCords/townCords.csv")
    return source_df.merge(cords, on=["Location"], how="left")

def _clean_location(value):
    if isinstance(value, str):
        return (value.lower()
                 .replace("foxcroft", "dover-foxcroft")
                 .strip())
    return value

def _clean_source_data(source_df):
    source_df["Source"] = source_df.loc(axis=1)["Publication","Date", "Premiums", "Description","Page #", "2nd Page #"].apply(lambda x: '|'.join(x.dropna().astype(str)), axis=1)

    source_df.drop(columns=["Publication","Date","Premiums", "Description","Page #", "2nd Page #"], inplace=True)

    source_df["Location"] = source_df["Location"].apply(_clean_location)

    source_df['rank'] = source_df.groupby(['Event', "Year"])['Source'].rank()

    pivot = source_df.pivot(index=["Year", "Event"], columns="rank", values="Source")

    reformated_sources = source_df.merge(pivot, on=["Year", "Event"], how="left")

    reformated_sources.drop(columns=["Source", "rank"], inplace=True)

    reformated_sources.drop_duplicates(inplace=True)

    reformated_sources.rename(columns={1.0:"Source:1",2.0:"Source:2",3.0:"Source:3",4.0:"Source:4"}, inplace=True)

    return reformated_sources

def parse_sources():
    source_data = _get_data_from_sources()
    source_data = _clean_source_data(source_data)
    source_data = _add_cords(source_data)
    return source_data



# un used learner functions.

#     # removes duplicate sources
#     def remove_duped_sources(source_df):
#         source_df = add_event_ids(source_df)
#         source_df = add_source_year_ids(source_df)
#         sources_key = pd.read_csv("source keys/key_for_sources.csv")
#         merged = (source_df
#                   .merge(sources_key, how="left", on="unique source ID", suffixes=("_old", ""))
#                   .fillna(source_df).drop(columns=["Page #_old", "2nd Page #_old", "Description_old", 'Premiums_old'])
#                   .drop_duplicates())
#         return (merged
#                     [~((merged["unique source ID"] == "1881-10-11 00:00:00,oxford democrat,1881,oxford county fair")
#                      & (merged["Description"] == "description"))]).drop(["unique source ID", "Event ID"], axis=1)
#
#     # add a Event ID each an instance of a year and a fair
#     def add_event_ids(source_df):
#         source_df['Event ID'] = source_df[["Year","Event"]].apply(
#             lambda x: ','.join(x.dropna().astype(str)),
#             axis=1)
#         return source_df
#
#     # adds and id that is for each a specific Event a specific data and specific source
#     def add_source_year_ids(source_df):
#         source_df['unique source ID'] = source_df[["Date","Source", "Event ID"]].apply(
#             lambda x: ','.join(x.dropna().astype(str)),
#             axis=1)
#         return source_df