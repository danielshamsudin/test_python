import glob
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import io
import xlsxwriter
import warnings
import time
from PIL import Image
from datetime import datetime

warnings.filterwarnings("ignore")


# utils #
def log(msg: str):
    print(f"#> {msg}")


# ---- #

# main #
dirpath = "input/"
inFile = glob.glob(f"{dirpath}/*.xlsx")

dfs = []
log(f"Reading all Excel files from {dirpath}")
for f in inFile:
    log(f"Found {f}")
    log(f"Reading {f}")
    tmp = pd.read_excel(f, sheet_name="RAW DATA")
    dfs.append(tmp)

mergedDf = pd.DataFrame(columns=dfs[0].columns)

for df in dfs:
    dftc = df[~df["Time"].isin(mergedDf["Time"])]
    mergedDf = pd.concat([mergedDf, dftc])

mergedDf = mergedDf[
    [
        "Time",
        "Site Location Name",
        "Site Location ID",
        "Region",
        "Cluster ID",
        "Cell Availability (Excl Cell Block)(%)",
    ]
]
log(f"Merged dataset success. Total number of rows: {len(mergedDf)}")

res = pl.from_pandas(mergedDf)

log("Creating view for Southern and Eastern Region")
df = res
df2s = df.filter(pl.col("Region") == "SOUTHERN")
df2e = df.filter(pl.col("Region") == "EASTERN")
df2s = df2s.group_by("Time", "Site Location Name", "Site Location ID").agg(
    pl.col("Cell Availability (Excl Cell Block)(%)").mean()
)
df2e = df2e.group_by("Time", "Site Location Name", "Site Location ID").agg(
    pl.col("Cell Availability (Excl Cell Block)(%)").mean()
)
df2s = df2s.filter(pl.col("Cell Availability (Excl Cell Block)(%)") <= 99.6)
df2e = df2e.filter(pl.col("Cell Availability (Excl Cell Block)(%)") <= 99.6)
df2s = df2s.sort(pl.col("Time"))
df2e = df2e.sort(pl.col("Time"))

df2s = df2s.with_columns(
    [
        pl.col("Cell Availability (Excl Cell Block)(%)").map_elements(
            lambda x: round(x, 2)
        )
    ]
)
df2e = df2e.with_columns(
    [
        pl.col("Cell Availability (Excl Cell Block)(%)").map_elements(
            lambda x: round(x, 2)
        )
    ]
)

df2s = df2s.pivot(
    index=["Site Location ID", "Site Location Name"],
    columns="Time",
    values="Cell Availability (Excl Cell Block)(%)",
    aggregate_function="mean",
)

df2s = df2s.filter(~pl.all_horizontal(pl.all().is_null()))
for cols in df2s.columns:
    df2s = df2s.with_columns([pl.col(cols).cast(pl.Utf8)])

df2e = df2e.pivot(
    index=["Site Location ID", "Site Location Name"],
    columns="Time",
    values="Cell Availability (Excl Cell Block)(%)",
    aggregate_function="mean",
)

df2e = df2e.filter(~pl.all_horizontal(pl.all().is_null()))
for cols in df2e.columns:
    df2e = df2e.with_columns([pl.col(cols).cast(pl.Utf8)])

df2s = df2s.fill_null("-")
df2e = df2e.fill_null("-")

log("View creation completed.")

log("Creating view for regions (mixed)")
# display constant baseline
# eastern 99.3
# southern 99.6
# format
# | date | Eastern baseline | MOCN (Eastern) | | | date | Southern.baseline | MOCN (Southern) |  # noqa: E501
#
rdf = res
rdf = rdf.group_by("Time", "Region").agg(
    pl.col("Cell Availability (Excl Cell Block)(%)").mean()
)
rdf = rdf.sort(pl.col("Time"))
rdf = rdf.with_columns(
    [
        pl.col("Cell Availability (Excl Cell Block)(%)").map_elements(
            lambda x: round(x, 2)
        )
    ]
)
rdfs = rdf.filter(pl.col("Region") == "SOUTHERN")
rdfe = rdf.filter(pl.col("Region") == "EASTERN")
sa = [99.6] * len(rdfs["Time"])
rdfs = rdfs.with_columns(pl.Series(name="Southern Baseline", values=sa)).rename(
    {"Cell Availability (Excl Cell Block)(%)": "Availability (%)"}
)
ea = [99.3] * len(rdfe["Time"])
rdfe = rdfe.with_columns(pl.Series(name="Eastern Baseline", values=ea)).rename(
    {"Cell Availability (Excl Cell Block)(%)": "Availability (%)"}
)
rdfs = rdfs.select(["Time", "Region", "Southern Baseline", "Availability (%)"])
rdfe = rdfe.select(["Time", "Region", "Eastern Baseline", "Availability (%)"])
log("View creation completed.")

log("Creating view for Site Location Name")
sdf = res
sdf = sdf.group_by("Time", "Site Location Name", "Site Location ID", "Region").agg(
    pl.col("Cell Availability (Excl Cell Block)(%)").mean()
)
sdf = sdf.sort(pl.col("Time"))
sdf = sdf.with_columns(
    [
        pl.col("Cell Availability (Excl Cell Block)(%)").map_elements(
            lambda x: round(x, 2)
        )
    ]
)
sdf = sdf.pivot(
    index=["Site Location Name", "Site Location ID", "Region"],
    columns="Time",
    values="Cell Availability (Excl Cell Block)(%)",
    aggregate_function="mean",
)
log("View creation completed.")

log("Creating view for Cluster ID")
cdf = res
cdf = cdf.group_by("Time", "Cluster ID").agg(
    pl.col("Cell Availability (Excl Cell Block)(%)").mean()
)
cdf = cdf.sort(pl.col("Time"))
cdf = cdf.with_columns(
    [
        pl.col("Cell Availability (Excl Cell Block)(%)").map_elements(
            lambda x: round(x, 2)
        )
    ]
)
cdf = cdf.pivot(
    index="Cluster ID",
    columns="Time",
    values="Cell Availability (Excl Cell Block)(%)",
    aggregate_function="mean",
)
log("View creation completed.")


fname = f"Site_Availability_{datetime.now().strftime('%Y%m%d')}.xlsx"
with xlsxwriter.Workbook(fname) as wb:
    log(f"Writing to file : {fname}")

    ws = wb.add_worksheet("region")
    rdfe.write_excel(wb, worksheet="region")
    rdfs.write_excel(wb, worksheet="region", position=(0, len(rdfe.columns) + 2))
    # graph for region
    log("Generating Line Chart for Southern and Eastern region")
    plt.figure(figsize=(10, 5))
    plt.plot(
        rdfe["Time"],
        rdfe["Availability (%)"],
        marker="o",
        linestyle="-",
        label="MOCN (Southern)",
    )
    plt.plot(
        rdfe["Time"],
        [99.6] * len(rdfe["Time"]),
        color="red",
        linestyle="-.",
        label="Sites baseline",
    )

    plt.xlabel("Date")
    plt.ylabel("Availability (%)")
    plt.xticks(rotation=90)
    plt.title("Southern Availability")
    plt.legend(loc="lower left")
    maxA = (
        rdfe["Availability (%)"].max(),
        rdfe["Availability (%)"].arg_max(),
    )
    minA = (
        rdfe["Availability (%)"].min(),
        rdfe["Availability (%)"].arg_min(),
    )
    plt.text(maxA[1] + 0.1, maxA[0], maxA[0], ha="left", color="green")
    plt.text(minA[1] + 0.1, minA[0], minA[0], ha="left", color="red")
    plt.text(len(rdfe["Time"]) - 1, 99.6, 99.6, ha="left", color="blue")
    imgstream = io.BytesIO()
    plt.savefig(imgstream, format="png", bbox_inches="tight")
    imgstream.seek(0)
    ws.insert_image(0, 2 * len(rdfs.columns) + 4, "", {"image_data": imgstream})
    imgstream = ""

    plt.figure(figsize=(10, 5))
    plt.plot(
        rdfs["Time"],
        rdfs["Availability (%)"],
        marker="o",
        linestyle="-",
        label="MOCN (Eastern)",
    )
    plt.plot(
        rdfs["Time"],
        [99.3] * len(rdfs["Time"]),
        color="red",
        linestyle="-.",
        label="Sites baseline",
    )
    plt.xlabel("Date")
    plt.ylabel("Availability (%)")
    plt.xticks(rotation=90)
    plt.title("Eastern Availability")
    plt.legend(loc="lower left")
    maxA = (
        rdfs["Availability (%)"].max(),
        rdfs["Availability (%)"].arg_max(),
    )
    minA = (
        rdfs["Availability (%)"].min(),
        rdfs["Availability (%)"].arg_min(),
    )
    plt.text(maxA[1] + 0.1, maxA[0], maxA[0], ha="left", color="green")
    plt.text(minA[1] + 0.1, minA[0], minA[0], ha="left", color="red")
    plt.text(len(rdfs["Time"]) - 1, 99.3, 99.3, ha="left", color="blue")
    imgstream = io.BytesIO()
    plt.savefig(imgstream, format="png", bbox_inches="tight")

    imgstream.seek(0)
    img = Image.open(imgstream)
    ws.insert_image("M29", "", {"image_data": imgstream})
    log("Graph creation completed.")
    # end graph
    ws = wb.add_worksheet("site")
    ws = wb.add_worksheet("cluster")
    ws = wb.add_worksheet("southern <= 99.6")
    ws = wb.add_worksheet("eastern <= 99.6")
    df2s.write_excel(wb, worksheet="southern <= 99.6")
    df2e.write_excel(wb, worksheet="eastern <= 99.6")
    sdf.write_excel(wb, worksheet="site")
    cdf.write_excel(wb, worksheet="cluster")

    log("Data write completed.")

time.sleep(5)
