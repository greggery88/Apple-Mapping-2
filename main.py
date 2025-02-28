import numpy as np
import pandas as pd
import os
import time


def get_date_from_county_fairs():
    # for counts prints
    start = time.time()
    t = 0

    # rows for file construction
    rows = []

    # gets a list of the apple data files.
    file_list = os.listdir("rawFairSourcesData")

    for county in file_list:
        # update counts and start a per file timer.
        file_start_time = time.time()
        t += 1

        sheet_names = pd.ExcelFile(f"Maine Fairs/{county}").sheet_names.remove("Varieties List")
        print(sheet_names)

if __name__ == '__main__':
    get_date_from_county_fairs()