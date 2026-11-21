# ETL Project Sample: Databricks Medallion Pipeline

A small end-to-end ETL project on Databricks. It reads order data from a Unity Catalog **Volume** and builds **bronze**, **silver** and **gold** Delta tables (medallion architecture).

## Architecture

```
Volume (CSV)  ->  Bronze  ->  Silver  ->  Gold
raw files         raw copy    cleaned     business
                  + audit     + typed     aggregates
                  columns     + deduped
```

| Layer | Table | What it holds |
|-------|-------|---------------|
| Bronze | `bronze_orders` | Raw data as strings, column names cleaned, plus `_ingest_ts` and `_source_file`. Loaded incrementally with Auto Loader. |
| Silver | `silver_orders` | Trimmed text, typed columns (int, decimal, date), invalid rows removed, one row per `order_id`, plus `total_amount`. |
| Gold | `gold_daily_sales` | Orders, units sold and revenue per day and product (cancelled orders excluded). |
| Gold | `gold_customer_summary` | Order count, lifetime value, first and last order date per customer (cancelled orders excluded). |

## Repository layout

```
.
├── README.md
├── .gitignore
├── data/
│   └── orders_sample.csv        # tiny fake sample for testing
└── notebooks/
    └── medallion_etl.py         # Databricks notebook (source format)
```

## Input format

CSV with a header row and these columns:

`order_id, customer_id, product, quantity, unit_price, order_date, status`

## Setup

1. A Databricks workspace with **Unity Catalog** enabled and permission to create tables and volumes in your catalog and schema.
2. Upload `data/orders_sample.csv` to your volume, for example `/Volumes/workspace/default/data/ETLSample/`.
3. Import `notebooks/medallion_etl.py` into Databricks (or open it through a Git folder).
4. Attach **Serverless** compute, or a cluster on Databricks Runtime 13.3 LTS or newer with Unity Catalog.

## Configuration

Edit the first code cell of the notebook:

| Setting | Default | Meaning |
|---------|---------|---------|
| `CATALOG` | `workspace` | Unity Catalog catalog |
| `SCHEMA` | `default` | Schema for volume and tables |
| `VOLUME` | `data` | Volume that holds the source files |
| `SOURCE_SUBPATH` | `ETLSample` | Folder inside the volume |
| `SOURCE_FILE_PATTERN` | `orders_sample.csv` | Only files matching this name are read |
| `FILE_FORMAT` | `csv` | `csv`, `json` or `parquet` |

## Run order

1. Run the config cell.
2. Run the reset cell **once** (clears checkpoints and drops the pipeline tables). Skip it on later runs if you want incremental loading.
3. Run bronze, then the column check, then silver, then gold.

To run on a schedule, create a Databricks **Job** with this notebook as the task.

## Expected results for the sample file

- `silver_orders`: 5 rows, with `total_amount` calculated.
- `gold_daily_sales`: order 1004 (cancelled) is excluded, so the 2026-11-03 monitor row does not appear.
- `gold_customer_summary`: C001 has 2 orders worth 895.00, C002 has 2 orders worth 881.00, and C003 does not appear because its only order was cancelled.

## Notes

- Do not commit real data files. `.gitignore` blocks CSV, JSON and Parquet files except the sample.
- Never commit tokens or credentials.
