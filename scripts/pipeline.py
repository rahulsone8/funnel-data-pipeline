import pandas as pd
import glob
import os
import shutil
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, FLOAT

# --------------------------------------------------
# Logging
# --------------------------------------------------

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Pipeline started")

# --------------------------------------------------
# Folders
# --------------------------------------------------

RAW = "data/raw"
ARCHIVE = "data/archive"
PROCESSED = "data/processed"

os.makedirs(ARCHIVE, exist_ok=True)
os.makedirs(PROCESSED, exist_ok=True)

# --------------------------------------------------
# MySQL Connection
# --------------------------------------------------

engine = create_engine(
    "mysql+pymysql://root:Rahul1975@localhost/funnel_pipeline"
)

# --------------------------------------------------
# Load Shops
# --------------------------------------------------

shops = pd.read_csv(f"{RAW}/shops.csv")
shops.columns = shops.columns.str.strip()

shops = shops.rename(columns={
    "GFID": "shop_id",
    "Store name": "store_name"
})

# --------------------------------------------------
# Load Funnel Files
# --------------------------------------------------

funnel_files = glob.glob(f"{RAW}/funnelanalysis*.csv")

events_list = []

for file in funnel_files:
    df = pd.read_csv(file)
    df.columns = df.columns.str.strip()
    events_list.append(df)

events = pd.concat(events_list, ignore_index=True)

# --------------------------------------------------
# Fix Timestamp
# --------------------------------------------------

events["Timestamp"] = events["Timestamp"].astype(str)
events["Timestamp"] = events["Timestamp"].str.split(",").str[0]

events["Timestamp"] = pd.to_datetime(
    events["Timestamp"],
    errors="coerce"
)

events = events.dropna(subset=["Timestamp"])

events["date"] = events["Timestamp"].dt.date
events["time"] = events["Timestamp"].dt.strftime("%H:%M:%S")

# --------------------------------------------------
# Clean Customer Numbers
# --------------------------------------------------

events["Customer Number"] = (
    events["Customer Number"]
    .astype(str)
    .str.replace(".0", "", regex=False)
)

# --------------------------------------------------
# Join Shops
# --------------------------------------------------

events = events.merge(
    shops,
    left_on="Shop ID",
    right_on="shop_id",
    how="left"
)

events = events.dropna(subset=["store_name"])

funnel_data = events[
[
"Action",
"date",
"time",
"Customer Number",
"store_name",
"shop_id"
]
]

funnel_data = funnel_data.rename(columns={
"Action": "action",
"Customer Number": "customer_number"
})

# --------------------------------------------------
# Load Order / Response Files
# --------------------------------------------------

response_files = glob.glob(f"{RAW}/response_*.csv") + glob.glob(f"{RAW}/data*.csv")

response_list = []

for file in response_files:
    df = pd.read_csv(file)
    df.columns = df.columns.str.strip()
    response_list.append(df)

response = pd.concat(response_list, ignore_index=True)

# --------------------------------------------------
# Process Orders
# --------------------------------------------------

response["Created"] = pd.to_datetime(response["Created"], errors="coerce")

response["date"] = response["Created"].dt.date
response["time"] = response["Created"].dt.strftime("%H:%M:%S")

orders_small = response[
[
"Order ID",
"Customer Contact",
"Delivery Pincode",
"Amount Paid by Customer",
"Customer Name"
]
]

orders_small = orders_small.rename(columns={
"Order ID": "pos_order_id",
"Customer Contact": "customer_number",
"Delivery Pincode": "delivery_address",
"Amount Paid by Customer": "total_order_value",
"Customer Name": "customer_name"
})

orders_small["customer_number"] = orders_small["customer_number"].astype(str)

# --------------------------------------------------
# Merge Orders With Funnel
# --------------------------------------------------

merged = funnel_data.merge(
    orders_small,
    on="customer_number",
    how="left"
)

mask = merged["action"] != "ORDER"

merged.loc[mask, "pos_order_id"] = None
merged.loc[mask, "delivery_address"] = None
merged.loc[mask, "total_order_value"] = None
merged.loc[mask, "customer_name"] = None

orders_only = merged[merged["pos_order_id"].notna()].drop_duplicates(subset=["pos_order_id"])
non_orders = merged[merged["pos_order_id"].isna()]

final_funnel = pd.concat([non_orders, orders_only], ignore_index=True)

# --------------------------------------------------
# Create Event Datetime
# --------------------------------------------------

final_funnel["event_datetime"] = pd.to_datetime(
    final_funnel["date"].astype(str) + " " + final_funnel["time"]
)

# --------------------------------------------------
# Get Last Loaded Timestamp
# --------------------------------------------------

query = """
SELECT MAX(CONCAT(date,' ',time)) AS last_timestamp
FROM funnel_data
"""

try:
    last_timestamp_df = pd.read_sql(query, engine)
    last_timestamp = last_timestamp_df.iloc[0]["last_timestamp"]
except:
    last_timestamp = None

# --------------------------------------------------
# Incremental Filter
# --------------------------------------------------

if last_timestamp is not None:

    final_funnel = final_funnel[
        final_funnel["event_datetime"] > pd.to_datetime(last_timestamp)
    ]

final_funnel = final_funnel.drop(columns=["event_datetime"])

logging.info(f"New rows detected: {len(final_funnel)}")

# --------------------------------------------------
# Save Processed Dataset
# --------------------------------------------------

final_funnel.to_csv(
    f"{PROCESSED}/funnel_data.csv",
    index=False
)

# --------------------------------------------------
# Insert Into MySQL
# --------------------------------------------------

if len(final_funnel) > 0:

    final_funnel.to_sql(
        "funnel_data",
        engine,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
        dtype={
            "action": VARCHAR(50),
            "date": VARCHAR(20),
            "time": VARCHAR(20),
            "customer_number": VARCHAR(20),
            "store_name": VARCHAR(255),
            "shop_id": VARCHAR(50),
            "pos_order_id": VARCHAR(50),
            "delivery_address": VARCHAR(20),
            "total_order_value": FLOAT,
            "customer_name": VARCHAR(255)
        }
    )

    logging.info("New rows inserted")

else:

    logging.info("No new rows to insert")

# --------------------------------------------------
# Archive Raw Files
# --------------------------------------------------

for file in funnel_files:

    filename = os.path.basename(file)
    shutil.move(file, f"{ARCHIVE}/{filename}")

for file in response_files:

    filename = os.path.basename(file)
    shutil.move(file, f"{ARCHIVE}/{filename}")

logging.info("Files archived")

print("Pipeline completed successfully")
logging.info("Pipeline finished")