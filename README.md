# Coraline Challenge (Data Engineer)

## Project Overview
This project addresses the **Coraline Challenge for Data Engineer Position**.  
It implements:
1. A **Python ingestion pipeline** to load `FoodSales` data from the provided Excel file (`[For candidate] de_challenge_data.xlsx`) into PostgreSQL.  
2. A **SQL transformation step** to generate a summary table `cat_reg` from the ingested data.  

The solution ensures:
- Compliance with requirements in the challenge statement.  
- Data quality validation and idempotency.  
- Clear logging and error handling.  
- Flexibility via `.env` and CLI parameters.  

---

## Tech Stack
- **Python 3.9.3** → pandas, SQLAlchemy, openpyxl, python-dotenv  
- **PostgreSQL 17** → target database  
- **Docker & Docker Compose** → containerized environment  
- **Logging** → timezone set to `Asia/Bangkok`  

---

## Project Structure
```
coraline-de-challenge/
├── data/
│   └── [For candidate] de_challenge_data.xlsx   # source Excel file
├── ingest/
│   └── ingest_foodsales.py        # ingestion script (Python)
├── sql/
│   └── create_cat_reg.sql         # SQL script for summary table
├── .env                           # environment variables
├── .gitignore                     # ignore venv, cache, logs, OS junk
├── docker-compose.yml             # compose configuration
├── Dockerfile                     # Python image build config
├── README.md                      # documentation
└── requirements.txt               # Python dependencies

```

---

## Environment Variables
| Variable      | Description                              | Example                          |
|---------------|------------------------------------------|----------------------------------|
| PGHOST        | Postgres host                            | `localhost` (CLI) / `postgres` (docker) |
| PGPORT        | Postgres port                            | `5432`                           |
| PGDATABASE    | Database name                            | `challenge`                      |
| PGUSER        | Database user                            | `root`                           |
| PGPASSWORD    | Database password                        | `DataEngineer_2024`              |
| EXCEL_PATH    | Path to Excel file                       | `./data/[For candidate] de_challenge_data.xlsx` |
| EXCEL_SHEET   | Excel sheet name                         | `FoodSales`                      |
| HEADER_ROW    | Header row index (0-based in pandas)     | `1`                              |
| CHUNKSIZE     | Batch size for `to_sql` inserts          | `20000`                          |
| SCHEMA        | Target schema                            | `public`                         |
| TABLE         | Target production table                  | `food_sales`                     |
| STAGE_TABLE   | Temporary staging table                  | `food_sales_staging`             |

---

## How to Run

### 1. Run via Docker Compose (Recommended)
```bash
docker compose up --build ingest
```
This will:
1. Start PostgreSQL (`postgres:17`)
2. Run the ingestion job after Postgres is healthy
3. Load Excel → staging → merge into production table `food_sales`
4. (Optional) View the logs of the ingestion service to monitor progress:
  ```bash
    docker compose logs -f ingest
  ```

### 2. Run via CLI (Local Python)
#### Create a virtual environment and install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```
Then, you can run the ingestion script in two ways:
#### 🔹Option A: Use config values from `.env` (easiest)
```bash
python ingest/ingest_foodsales.py
```
#### 🔹 Option B: Provide arguments to override `.env` values
```bash
python ingest/ingest_foodsales.py \
--excel-path "./data/[For candidate] de_challenge_data.xlsx" \
--sheet FoodSales \
--header-row 1 \
--chunksize 20000 \
--schema public \
--table food_sales \
--stage-table food_sales_staging
```
Any argument not specified will automatically fallback to the values in `.env`.

### 3. Run SQL Script to Create Summary Table
```bash
psql -U root -d challenge -f sql/create_cat_reg.sql
```
This generates the table `cat_reg`.

---

## Database Schema

**Production Table: `public.food_sales`**
```sql
id         TEXT PRIMARY KEY,
date       DATE,
region     TEXT,
city       TEXT,
category   TEXT,
product    TEXT,
qty        INTEGER,
unitprice  NUMERIC(18,2),
totalprice NUMERIC(18,2),
CHECK (qty >= 0),
CHECK (unitprice >= 0),
CHECK (totalprice >= 0)
```

Indexes:
- `date`
- `region`
- `city`
- `(category, product)`

---

## Workflow
1. **Read Excel** → pandas DataFrame  
2. **Validation** → enforce schema, filter invalid/negative values  
3. **Load Staging Table** → truncate & insert clean data  
4. **Merge** → `INSERT ... ON CONFLICT DO NOTHING` into `food_sales`  
5. **Drop Staging** → cleanup after load  
6. **Transformation SQL** → create summary table `cat_reg`  
7. **Logging** → detailed INFO/WARN, timezone = Asia/Bangkok  

---

## SQL Transformation – `cat_reg`

The summary table `cat_reg` is created using a **dynamic SQL script**. The logic works as follows:
- It drops the existing `cat_reg` table if it exists (idempotent behavior).
- It queries distinct `region` values from `food_sales`.
- For each region, it dynamically builds an expression of the form:  
  *“SUM totalprice for rows where region = X, labeled as column X”*.
- All region-specific expressions are concatenated into a single dynamic SQL string.
- The final query groups data by `category`, includes one column per region, and adds a `Grand Total`.
- The script executes this dynamic SQL, creating the `cat_reg` table with pivoted results.

This approach ensures the solution adapts automatically to any number of regions without modifying the SQL manually.

### Example Output
| Category | East   | West  | Grand Total |
|----------|--------|-------|-------------|
| Bars     | 6,355  | 4,180 | 10,536      |
| Cookies  | 10,684 | 6,529 | 17,212      |
| Crackers | 3,026  | 314   | 3,340       |
| Snacks   | 1,460  | 778   | 2,238       |

---

## Sample Log Output (Ingestion)
```
2025-09-09 08:12:44 INFO [READ] Start ingest: file=./data/[For candidate] de_challenge_data.xlsx sheet=FoodSales header_row=1
2025-09-09 08:12:44 INFO [READ] Rows after cleaning/validation: 244
2025-09-09 08:12:44 INFO [INIT] Ensured schema/tables
2025-09-09 08:12:45 INFO [STAGE] Loaded into: public.food_sales_staging
2025-09-09 08:12:45 INFO [MERGE] To prod table: public.food_sales, inserted=244
2025-09-09 08:12:45 INFO [STAGE] Dropped stage table: public.food_sales_staging
2025-09-09 08:12:45 INFO [GOAL] DONE: rows_in=244, rows_inserted=244, target_table=public.food_sales
```

---

## Extendability
- Replace Excel input with CSV/Parquet → add new reader functions  
- Add validation rules (e.g., business logic constraints)  
- Extend SQL transformation to pivot by city or year for more granular reporting  

---

## Deliverables 
- Source code (this repository) 
- Dockerfile & docker-compose.yml 
- README.md (this documentation)

---
## Author
- **Candidate:** *Thatsaphon Losuwannarak*
- GitHub: [thatsaphon2745](https://github.com/thatsaphon2745)  
- Submission for **Coraline Challenge (Data Engineer)**
