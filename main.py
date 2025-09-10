from cleanAppleNames import clean_apple_names
from mergeSourceAndApples import merge_sources_n_apples
from parseSources import parse_sources
from parseAppleData import parse_apple
from utilities import *
import numpy as np

"""
MAIN:

this is the main function it is the highest level function it tell the code to start and what order to do things in

first is make a logger is also call the log in other places this allows the program to print out error so we can see them.

next it tell the program to execute the gathering sources fase if finish by gather thing the apple data.

*** ------------------------------- ***


READING ERRORS:

error and warning are out putted to the log there are three kind of logs messages:

1. Info: this tell you information about where in the program is currently working and what its doing next.

2. Debug: this is information that is good to know but has no affect on large parts of the project these are not really problems

3. Warning: this are places where the program has run into an error it should be able to solve.

4. Error: These are larger problem that the program cannot solve and might need to be fix manual. However, they done break the hole project 

5. Critical: these are error that break the program.

debug format for log messages: (this is mostly for errors and warnings)

this format is standardized for this project to this format and goes.

# line 1 #
    Format { (Name of general error): (specific identifier of error)> }

    this is the title of the error and describes what is coursing the problem

    example: No column header in sheet matching: Year>

    This example error describes tell us an expected column header is not present on the Excel Sheet in 
    Question. this is the type of error described the (Year) tells us that the header that is missing
    is the year header.
# ------ #

# line 2 #          (this can take up multiple lines) 
    Format { - Info: (title of any information useful to finding the exact problem) = the information }

    this is extra information about the problem. use to identify the cause of the problem so that 
    it can be fixed fully.

    Example: Info: Current_headers are = [Years, Source, Source Date, Page, Notes]

    reading this example is connect to line one we see that the reason Year was not present in the 
    headers list was because it has been pluralised.
# ------ #

# line 3 #
    Format: { - Loc: title of location = (location e.x. (file_name, sheet, year))}

    this give the location of the data in the original data set.

    Example: Loc: file = {county}, sheet = {sheet}
# ------ #

# line 4 #
    Format: { - Fix: the quick fix to the problem that is currently implement to allow the program to continue running }

    tell what the program is doing to get around the problem.

    Example: by-pass this sheet all data from {sheet} will not be recorded.
# ------ #

***  --------------------------------  ***

"""


def main() -> None:
    """this is the main function it tells the order for everything to happen it.
    it starts by setting up a logger, which is the system that prints out information
    about the process, warnings, and errors.
    """
    log = setup_logging("Main log")  # this is the logger setup!

    log.info("make config")
    gs_config = get_gs_config("JackClientIDs.json")
    # calls the function that parses the Apple data.
    apple_df = parse_apple(log, gs_config)
    log.info(f"apple dataframe events are cleaned")

    apple_df.replace("", np.nan, inplace=True)

    log.info("cleaning apple names")
    apple_df = clean_apple_names(
        log,
        apple_df,
        separate_presumed_names=False,
        separate_given_names=False,
        separate_alt_names=False,
    )
    log.info("cleaned apple names!")

    # calls the function that parses the sources.
    source_df = parse_sources(log, gs_config, split_sources=False)
    log.info(f"source dataframe events are cleaned")

    # merge sources and Apple's data.
    data = merge_sources_n_apples(log, apple_df, source_df)

    # send to csv
    to_csv_file(df=data, path="cleanedData/Full", name="data")
    clean_spread = Spread(
        spread=f"CleanedData_{get_datetime()}", create_spread=True, config=gs_config
    )
    clean_spread.move(path="/TestFilesApples/OutputData/")
    clean_spread.df_to_sheet(data, index=False, headers=True)
    spreadsheet_url = clean_spread.url
    print(f"Spreadsheet URL: {spreadsheet_url}")
    log.info(f"final data written to data.csv")


if __name__ == "__main__":
    main()
