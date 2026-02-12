import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import logging
from sqlalchemy import create_engine

# ---------------- CONFIG ----------------

CSV_FILE = "sales_data.csv"
JSON_FILE = "sales_data.json"
PARQUET_FILE = "sales_data.parquet"
DB_FILE = "sales.db"

# ---------------- LOGGING ----------------

logging.basicConfig(
    filename="etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def call_api():
    """Simulated API Call"""
    logging.info("Calling API")

    response = requests.get("https://jsonplaceholder.typicode.com/todos/1")

    if response.status_code == 200:
        logging.info("API Call Successful")
    else:
        logging.warning("API Call Failed")

def transform_data(df):
    """Pandas Transformations"""
    logging.info("Transforming Data")

    df["total_amount"] = df["price"] * df["quantity"]

    city_sales = df.groupby("city")["total_amount"].sum().reset_index()

    logging.info("Transformation Complete")

    return df, city_sales

def write_parquet(df):
    """PyArrow Parquet Write"""
    logging.info("Writing Parquet File")

    table = pa.Table.from_pandas(df)
    pq.write_table(table, PARQUET_FILE)

    logging.info("Parquet File Created")

def load_to_db(df, city_sales):
    """Database Load with Transaction"""
    logging.info("Loading Data into Database")

    engine = create_engine(f"sqlite:///{DB_FILE}")

    with engine.begin() as connection:  # ✅ Transaction management
        df.to_sql("sales", connection, if_exists="replace", index=False)
        city_sales.to_sql("city_sales", connection, if_exists="replace", index=False)

    logging.info("Database Load Complete")

def main():

    print("🚀 Python ETL Started")
    logging.info("ETL Job Started")

    try:
        # ---------------- FILE HANDLING ----------------

        logging.info("Reading CSV File")
        df = pd.read_csv(CSV_FILE)

        logging.info(f"Rows Loaded: {len(df)}")

        # Also read JSON (concept coverage)
        logging.info("Reading JSON File")
        json_df = pd.read_json(JSON_FILE)

        # ---------------- API CALL ----------------

        call_api()

        # ---------------- TRANSFORM ----------------

        df, city_sales = transform_data(df)

        # ---------------- PARQUET ----------------

        write_parquet(df)

        # ---------------- DATABASE ----------------

        load_to_db(df, city_sales)

        print("✅ ETL Completed Successfully")
        logging.info("ETL Job Finished Successfully")

    except Exception as e:
        print("❌ ERROR:", e)
        logging.error(f"ETL Failed: {str(e)}")

if __name__ == "__main__":
    main()
