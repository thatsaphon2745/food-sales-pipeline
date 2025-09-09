# ingest/ingest_foodsales.py
import os
import sys
import argparse
import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load key-value pairs from .env file into environment variables
# so we can access configs using os.getenv()
load_dotenv()

# -----------------------
# Config (ENV + CLI)
# -----------------------
def parse_args():
    p = argparse.ArgumentParser(description="Ingest FoodSales Excel into Postgres")
    p.add_argument("--excel-path", type=str, help="Path to Excel file")
    p.add_argument("--sheet", type=str, help="Sheet name")
    p.add_argument("--header-row", type=int, help="Header row index (0-based)")
    p.add_argument("--chunksize", type=int, help="to_sql chunksize")
    p.add_argument("--schema", type=str, help="DB schema (default: public)")
    p.add_argument("--table", type=str, help="Target prod table (default: food_sales)")
    p.add_argument("--stage-table", type=str, help="Staging table (default: food_sales_stage)")
    return p.parse_args()

args = parse_args()

# -----------------------
# Load config from .env and CLI
# -----------------------
PGHOST      = os.getenv("PGHOST")
PGPORT      = os.getenv("PGPORT")
PGDATABASE  = os.getenv("PGDATABASE")
PGUSER      = os.getenv("PGUSER")
PGPASSWORD  = os.getenv("PGPASSWORD")

EXCEL_PATH  = args.excel_path or os.getenv("EXCEL_PATH", "./data/foodsales.xlsx")
EXCEL_SHEET = args.sheet      or os.getenv("EXCEL_SHEET", "FoodSales")
HEADER_ROW  = args.header_row if args.header_row is not None else int(os.getenv("HEADER_ROW", "1"))
CHUNKSIZE   = args.chunksize  if args.chunksize is not None else int(os.getenv("CHUNKSIZE", "20000"))

SCHEMA      = args.schema     or os.getenv("SCHEMA", "public")
TABLE       = (args.table     or os.getenv("TABLE", "food_sales")).strip()
STAGE_TABLE = (args.stage_table or os.getenv("STAGE_TABLE", "food_sales_staging")).strip()

# Configure logging:
# - Use local time instead of UTC for timestamps
# - Set log level to INFO (show INFO and above)
# - Format: "YYYY-MM-DD HH:MM:SS LEVEL message"
logging.Formatter.converter = time.localtime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# -----------------------
# Helpers
# -----------------------
def read_foodsales(path: str, sheet: str, header_row: int) -> pd.DataFrame:
    """Read the FoodSales sheet and enforce schema with 9 required columns"""
    # Read Excel sheet into DataFrame
    df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl", header=header_row)

    # Validate required columns exist
    expected = ["ID", "Date", "Region", "City", "Category", "Product", "Qty", "UnitPrice", "TotalPrice"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in Excel: {missing}. Found: {list(df.columns)}")

    # Keep only expected columns
    df = df[expected].copy()

    # Cast columns to proper types (string, numeric, datetime)
    df["ID"]         = df["ID"].astype(str).str.strip()
    df["Date"]       = pd.to_datetime(df["Date"], errors="coerce")
    df["Region"]     = df["Region"].astype(str)
    df["City"]       = df["City"].astype(str)
    df["Category"]   = df["Category"].astype(str)
    df["Product"]    = df["Product"].astype(str)
    df["Qty"]        = pd.to_numeric(df["Qty"], errors="coerce").astype("Int64")
    df["UnitPrice"]  = pd.to_numeric(df["UnitPrice"], errors="coerce")
    df["TotalPrice"] = pd.to_numeric(df["TotalPrice"], errors="coerce")

    # Filter out empty rows or invalid header-like rows
    mask = (
        df["ID"].str.len().gt(0) &
        df["Date"].notna() &
        df["TotalPrice"].notna() &
        df["Region"].str.strip().ne("") &
        df["Category"].str.strip().ne("") &
        df["Product"].str.strip().ne("")
    )
    df = df.loc[mask].copy()

    # Convert datetime → date (drop time part)
    df["Date"] = df["Date"].dt.date

    # Rename columns to snake_case
    df.columns = ["id","date","region","city","category","product","qty","unitprice","totalprice"]

    # Basic validation: remove negative values, warn if any removed
    before = len(df)
    df = df[
        (df["qty"].isna() | (df["qty"] >= 0)) &
        (df["unitprice"].isna() | (df["unitprice"] >= 0)) &
        (df["totalprice"].isna() | (df["totalprice"] >= 0))
    ].copy()
    removed = before - len(df)
    if removed > 0:
        logging.warning("Filtered out %d rows due to negative qty/unitprice/totalprice", removed)

    return df


def ensure_schema_and_tables(con):
    ddl = f"""
    -- Create schema if it doesn't exist (idempotent)
    CREATE SCHEMA IF NOT EXISTS {SCHEMA};

    -- Create main production table if it doesn't exist
    CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
        id         text PRIMARY KEY,          -- unique record identifier
        date       date,                      -- sales date
        region     text,                      -- region name
        city       text,                      -- city name
        category   text,                      -- product category
        product    text,                      -- product name
        qty        integer,                   -- quantity sold
        unitprice  numeric(18,2),             -- price per unit
        totalprice numeric(18,2),             -- total sale value
        -- Data quality constraints: non-negative values
        CONSTRAINT chk_qty_nonneg CHECK (qty IS NULL OR qty >= 0),
        CONSTRAINT chk_unitprice_nonneg CHECK (unitprice IS NULL OR unitprice >= 0),
        CONSTRAINT chk_totalprice_nonneg CHECK (totalprice IS NULL OR totalprice >= 0)
    );

    -- Create indexes for faster query performance
    CREATE INDEX IF NOT EXISTS idx_{TABLE}_date     ON {SCHEMA}.{TABLE}(date);
    CREATE INDEX IF NOT EXISTS idx_{TABLE}_region   ON {SCHEMA}.{TABLE}(region);
    CREATE INDEX IF NOT EXISTS idx_{TABLE}_city     ON {SCHEMA}.{TABLE}(city);
    CREATE INDEX IF NOT EXISTS idx_{TABLE}_cat_prod ON {SCHEMA}.{TABLE}(category, product);
    """
    # Execute the DDL statements in the current transaction
    con.execute(text(ddl))


def load_stage(con, df: pd.DataFrame):
    """Load data into staging table: create if missing, truncate old data, then bulk insert"""
    # Create staging table if not exists, clone schema from production table
    con.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{STAGE_TABLE}
        (LIKE {SCHEMA}.{TABLE} INCLUDING ALL);
    """))

    # Clear old data from staging (fast truncate)
    con.execute(text(f"TRUNCATE TABLE {SCHEMA}.{STAGE_TABLE};"))

    # Bulk load DataFrame into staging table in chunks
    df.to_sql(
        name=STAGE_TABLE,
        con=con,
        schema=SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=CHUNKSIZE,
    )

def merge_stage_to_prod(con):
    """Insert-or-ignore from staging → production; skip duplicates by PK(id)"""
    sql = f"""
    -- Insert all rows from staging into production
    INSERT INTO {SCHEMA}.{TABLE} (id, date, region, city, category, product, qty, unitprice, totalprice)
    SELECT s.id, s.date, s.region, s.city, s.category, s.product, s.qty, s.unitprice, s.totalprice
    FROM {SCHEMA}.{STAGE_TABLE} s
    WHERE s.id IS NOT NULL
    -- If id already exists in production, ignore (no error, no update)
    ON CONFLICT (id) DO NOTHING;
    """
    # Execute and return number of successfully inserted rows
    res = con.execute(text(sql))
    return res.rowcount or 0

def main():
    # Log start of ingestion with input config
    logging.info(
        "[READ] Start ingest: file=%s sheet=%s header_row=%s",
        EXCEL_PATH, EXCEL_SHEET, HEADER_ROW
    )

    try:
        # Read and clean Excel into DataFrame
        df = read_foodsales(EXCEL_PATH, EXCEL_SHEET, HEADER_ROW)
    except Exception as e:
        # Exit if reading/cleaning Excel fails
        logging.exception("[READ] Failed to read/clean excel: %s", e)
        sys.exit(1)

    # Log number of valid rows after cleaning
    rows_in = len(df)
    logging.info("[READ] Rows after cleaning/validation: %d", rows_in)

    # Create SQLAlchemy engine for PostgreSQL
    eng = create_engine(
        f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
    )

    try:
        # Run all DB operations inside one transaction
        with eng.begin() as con:
            # Ensure schema and production table exist
            ensure_schema_and_tables(con)
            logging.info("[INIT] Ensured schema/tables")

            # Load cleaned data into staging table
            load_stage(con, df)
            logging.info("[STAGE] Loaded into: %s.%s", SCHEMA, STAGE_TABLE)

            # Insert-or-ignore from staging → production
            inserted = merge_stage_to_prod(con) or 0
            logging.info("[MERGE] To prod table: %s.%s, inserted=%d", SCHEMA, TABLE, inserted)

            # Drop staging table after merge
            con.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{STAGE_TABLE};"))
            logging.info("[STAGE] Dropped stage table: %s.%s", SCHEMA, STAGE_TABLE)

        # Log summary after successful ingestion
        logging.info(
            "[GOAL] DONE: rows_in=%d, rows_inserted=%d, target_table=%s.%s",
            rows_in, inserted, SCHEMA, TABLE
        )

    except Exception as e:
        # If any DB step fails, log error and exit with code 2
        logging.exception("[FAIL] Ingestion failed: %s", e)
        sys.exit(2)

if __name__ == "__main__":
    main()