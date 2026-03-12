import glob
import logging
import os
import shutil

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import FLOAT, VARCHAR

# --------------------------------------------------
# Logging
# --------------------------------------------------

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logging.info("Pipeline started")

# --------------------------------------------------
# Config
# --------------------------------------------------

RAW = "data/raw"
ARCHIVE = "data/archive"
PROCESSED = "data/processed"
CHUNK_SIZE = int(os.getenv("PIPELINE_CHUNK_SIZE", "5000"))
ORDER_STAGE_TABLE = "funnel_order_stage"

os.makedirs(ARCHIVE, exist_ok=True)
os.makedirs(PROCESSED, exist_ok=True)

engine = create_engine(
    os.getenv("FUNNEL_DB_URL", "mysql+pymysql://root:Rahul1975@localhost/funnel_pipeline")
)

FALLBACK_OUTPUT_COLUMNS = [
    "action",
    "date",
    "time",
    "customer_number",
    "store_name",
    "shop_id",
    "pos_order_id",
    "delivery_address",
    "total_order_value",
    "customer_name",
]


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def clean_customer_number(series):
    return series.astype(str).str.replace(".0", "", regex=False).str.strip()


def get_last_timestamp(db_engine):
    query = """
    SELECT MAX(CONCAT(date,' ',time)) AS last_timestamp
    FROM funnel_data
    """

    try:
        last_timestamp_df = pd.read_sql(query, db_engine)
        value = last_timestamp_df.iloc[0]["last_timestamp"]
        return pd.to_datetime(value) if pd.notna(value) else None
    except Exception as exc:
        logging.warning("Could not fetch last loaded timestamp: %s", exc)
        return None


def load_shops_map():
    shops = pd.read_csv(f"{RAW}/shops.csv")
    shops.columns = shops.columns.str.strip()

    shops = shops.rename(columns={"GFID": "shop_id", "Store name": "store_name"})
    shops["shop_id"] = shops["shop_id"].astype(str).str.strip()

    return shops[["shop_id", "store_name"]].drop_duplicates(subset=["shop_id"])


def reset_order_stage_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {ORDER_STAGE_TABLE} (
        customer_number VARCHAR(50),
        created DATETIME,
        pos_order_id VARCHAR(100),
        delivery_address VARCHAR(50),
        total_order_value DOUBLE,
        customer_name VARCHAR(255)
    )
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text(f"TRUNCATE TABLE {ORDER_STAGE_TABLE}"))


def stream_orders_to_stage(response_files):
    required_columns = {
        "Created",
        "Order ID",
        "Customer Contact",
        "Delivery Pincode",
        "Amount Paid by Customer",
        "Customer Name",
    }

    if not response_files:
        logging.info("No response files found; skipping order staging")
        return

    reset_order_stage_table()

    for file in response_files:
        logging.info("Reading response file in chunks: %s", file)

        for chunk in pd.read_csv(file, chunksize=CHUNK_SIZE):
            chunk.columns = chunk.columns.str.strip()
            missing = required_columns - set(chunk.columns)
            if missing:
                raise ValueError(f"Missing required columns in {file}: {sorted(missing)}")

            chunk = chunk[list(required_columns)].copy()
            chunk["Created"] = pd.to_datetime(chunk["Created"], errors="coerce")
            chunk = chunk.dropna(subset=["Created"])

            if chunk.empty:
                continue

            chunk = chunk.rename(
                columns={
                    "Created": "created",
                    "Order ID": "pos_order_id",
                    "Customer Contact": "customer_number",
                    "Delivery Pincode": "delivery_address",
                    "Amount Paid by Customer": "total_order_value",
                    "Customer Name": "customer_name",
                }
            )

            chunk["customer_number"] = clean_customer_number(chunk["customer_number"])
            chunk = chunk[
                [
                    "customer_number",
                    "created",
                    "pos_order_id",
                    "delivery_address",
                    "total_order_value",
                    "customer_name",
                ]
            ]

            chunk.to_sql(
                ORDER_STAGE_TABLE,
                engine,
                if_exists="append",
                index=False,
                chunksize=CHUNK_SIZE,
                method="multi",
            )


def fetch_latest_orders_for_customers(customer_numbers):
    if not customer_numbers:
        return pd.DataFrame(
            columns=[
                "customer_number",
                "pos_order_id",
                "delivery_address",
                "total_order_value",
                "customer_name",
            ]
        )

    params = {f"c{i}": c for i, c in enumerate(customer_numbers)}
    placeholders = ", ".join(f":c{i}" for i in range(len(customer_numbers)))

    query = text(
        f"""
        SELECT customer_number, pos_order_id, delivery_address, total_order_value, customer_name
        FROM (
            SELECT
                customer_number,
                pos_order_id,
                delivery_address,
                total_order_value,
                customer_name,
                ROW_NUMBER() OVER (PARTITION BY customer_number ORDER BY created DESC) AS rn
            FROM {ORDER_STAGE_TABLE}
            WHERE customer_number IN ({placeholders})
        ) ranked
        WHERE rn = 1
        """
    )

    return pd.read_sql(query, engine, params=params)


def build_output_chunk(events_chunk, shops_map, last_timestamp, seen_order_ids):
    events = events_chunk.copy()
    events.columns = events.columns.str.strip()

    required_columns = {"Timestamp", "Customer Number", "Shop ID", "Action"}
    missing = required_columns - set(events.columns)
    if missing:
        raise ValueError(f"Missing required columns in funnel chunk: {sorted(missing)}")

    events["Timestamp"] = events["Timestamp"].astype(str).str.split(",").str[0]
    events["Timestamp"] = pd.to_datetime(events["Timestamp"], errors="coerce")
    events = events.dropna(subset=["Timestamp"])

    if events.empty:
        return pd.DataFrame(columns=FALLBACK_OUTPUT_COLUMNS)

    events["customer_number"] = clean_customer_number(events["Customer Number"])
    events["shop_id"] = events["Shop ID"].astype(str).str.strip()
    events["action"] = events["Action"].astype(str).str.strip()
    events["date"] = events["Timestamp"].dt.date.astype(str)
    events["time"] = events["Timestamp"].dt.strftime("%H:%M:%S")

    events = events.merge(shops_map, on="shop_id", how="left")
    events = events.dropna(subset=["store_name"])

    if events.empty:
        return pd.DataFrame(columns=FALLBACK_OUTPUT_COLUMNS)

    if last_timestamp is not None:
        events = events[events["Timestamp"] > last_timestamp]

    if events.empty:
        return pd.DataFrame(columns=FALLBACK_OUTPUT_COLUMNS)

    output = events[
        ["action", "date", "time", "customer_number", "store_name", "shop_id"]
    ].copy()

    output["pos_order_id"] = None
    output["delivery_address"] = None
    output["total_order_value"] = None
    output["customer_name"] = None

    order_mask = output["action"] == "ORDER"
    if order_mask.any():
        order_customers = output.loc[order_mask, "customer_number"].dropna().unique().tolist()
        order_map = fetch_latest_orders_for_customers(order_customers)

        if not order_map.empty:
            output = output.merge(
                order_map,
                on="customer_number",
                how="left",
                suffixes=("", "_order"),
            )
            output["pos_order_id"] = output["pos_order_id_order"]
            output["delivery_address"] = output["delivery_address_order"]
            output["total_order_value"] = output["total_order_value_order"]
            output["customer_name"] = output["customer_name_order"]
            output = output.drop(
                columns=[
                    "pos_order_id_order",
                    "delivery_address_order",
                    "total_order_value_order",
                    "customer_name_order",
                ]
            )

    mask_non_order = output["action"] != "ORDER"
    output.loc[mask_non_order, ["pos_order_id", "delivery_address", "total_order_value", "customer_name"]] = None

    non_orders = output[output["pos_order_id"].isna()]

    orders_only = output[output["pos_order_id"].notna()].copy()
    if not orders_only.empty:
        orders_only = orders_only[~orders_only["pos_order_id"].isin(seen_order_ids)]
        seen_order_ids.update(orders_only["pos_order_id"].dropna().tolist())

    return pd.concat([non_orders, orders_only], ignore_index=True)


def insert_chunk(df):
    if df.empty:
        return 0

    df.to_sql(
        "funnel_data",
        engine,
        if_exists="append",
        index=False,
        chunksize=CHUNK_SIZE,
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
            "customer_name": VARCHAR(255),
        },
    )
    return len(df)


# --------------------------------------------------
# Pipeline
# --------------------------------------------------

def main():
    funnel_files = sorted(glob.glob(f"{RAW}/funnelanalysis*.csv"))
    response_files = sorted(glob.glob(f"{RAW}/response_*.csv") + glob.glob(f"{RAW}/data*.csv"))

    if not funnel_files:
        logging.info("No funnel files found; nothing to process")
        print("No funnel files found")
        return

    shops_map = load_shops_map()
    stream_orders_to_stage(response_files)
    last_timestamp = get_last_timestamp(engine)

    output_path = f"{PROCESSED}/funnel_data.csv"
    if os.path.exists(output_path):
        os.remove(output_path)

    total_inserted = 0
    header_written = False
    seen_order_ids = set()

    for file in funnel_files:
        logging.info("Reading funnel file in chunks: %s", file)

        for chunk in pd.read_csv(file, chunksize=CHUNK_SIZE):
            prepared_chunk = build_output_chunk(chunk, shops_map, last_timestamp, seen_order_ids)

            if prepared_chunk.empty:
                continue

            inserted = insert_chunk(prepared_chunk)
            total_inserted += inserted

            prepared_chunk.to_csv(
                output_path,
                mode="a",
                header=not header_written,
                index=False,
            )
            header_written = True

            logging.info("Inserted %s rows from current chunk", inserted)

    for file in funnel_files + response_files:
        filename = os.path.basename(file)
        shutil.move(file, f"{ARCHIVE}/{filename}")

    logging.info("Files archived")
    logging.info("Pipeline finished with %s inserted rows", total_inserted)
    print(f"Pipeline completed successfully. Rows inserted: {total_inserted}")


if __name__ == "__main__":
    main()
