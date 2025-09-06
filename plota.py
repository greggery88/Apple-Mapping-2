from utilities import find_latest_file, setup_logging
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.widgets as wg
import geopandas as gpd


def main():
    fig, ax = plt.subplots()
    df = find_latest_file("Full")
    df = df.loc[df["Use Name"] == "baldwin"]
    gdf = gpd.GeoDataFrame(
        df,
        crs="EPSG:4326",
        geometry=gpd.points_from_xy(x=df["Longitude"], y=df["Latitude"]),
    )
    gdf.explore(ax=ax)


if __name__ == "__main__":
    main()
