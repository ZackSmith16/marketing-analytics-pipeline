"""
Build the pipeline: load raw data, build models in dependency order, run tests.

Usage:
    python pipeline/run.py --target local
        Runs everything on your machine with DuckDB. No cloud account needed.
        The BigQuery SQL is translated to DuckDB's dialect with sqlglot.

    python pipeline/run.py --target bigquery --project YOUR_GCP_PROJECT_ID
        Loads the raw files into BigQuery, builds every model as a table,
        and runs the tests. Works in the free BigQuery sandbox.

Steps run in a fixed order, so a model never reads a table that hasn't been
rebuilt yet. That replaces independent scheduled queries that could run
before the tables they depend on were refreshed.
"""

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
SQL = ROOT / "sql"
TESTS = ROOT / "tests"
REFERENCE = ROOT / "reference"

DATASETS = ["raw", "reference", "staging", "marts"]

# Models in build order: (dataset, table name, SQL file)
MODELS = [
    ("staging", "stg_clients", SQL / "staging/stg_clients.sql"),
    ("staging", "stg_google_ads__campaigns", SQL / "staging/stg_google_ads__campaigns.sql"),
    ("staging", "stg_meta_ads__campaigns", SQL / "staging/stg_meta_ads__campaigns.sql"),
    ("staging", "stg_meta_ads__actions", SQL / "staging/stg_meta_ads__actions.sql"),
    ("marts", "mart_campaign_daily", SQL / "marts/mart_campaign_daily.sql"),
    ("marts", "mart_client_daily", SQL / "marts/mart_client_daily.sql"),
    ("staging", "stg_shopify__orders", SQL / "staging/stg_shopify__orders.sql"),
    ("staging", "stg_shopify__line_items", SQL / "staging/stg_shopify__line_items.sql"),
    ("staging", "stg_shopify__refunds", SQL / "staging/stg_shopify__refunds.sql"),
    ("staging", "stg_ga4__sessions", SQL / "staging/stg_ga4__sessions.sql"),
    ("marts", "mart_shopify_orders", SQL / "marts/mart_shopify_orders.sql"),
    ("marts", "mart_shopify_daily", SQL / "marts/mart_shopify_daily.sql"),
    ("marts", "mart_shopify_product_daily", SQL / "marts/mart_shopify_product_daily.sql"),
    ("marts", "mart_ga4_source_daily", SQL / "marts/mart_ga4_source_daily.sql"),
    ("marts", "mart_ga4_channel_daily", SQL / "marts/mart_ga4_channel_daily.sql"),
]

# Raw files and their BigQuery schemas. Explicit schemas stop BigQuery's
# auto-detection from guessing wrong (e.g. reading long IDs as numbers).
RAW_TABLES = {
    "clients": {
        "file": DATA / "raw_clients.jsonl",
        "schema": [
            ("client_id", "STRING"), ("client_name", "STRING"), ("client_type", "STRING"),
            ("platform", "STRING"), ("account_id", "STRING"),
        ],
    },
    "google_ads_campaigns": {
        "file": DATA / "raw_google_ads_campaigns.jsonl",
        "schema": [
            ("date", "DATE"), ("account_id", "STRING"), ("campaign_id", "STRING"),
            ("campaign_name", "STRING"), ("impressions", "INT64"), ("clicks", "INT64"),
            ("cost", "FLOAT64"), ("conversions", "FLOAT64"), ("conversions_value", "FLOAT64"),
            ("_synced_at", "TIMESTAMP"),
        ],
    },
    "meta_ads_campaigns": {
        "file": DATA / "raw_meta_ads_campaigns.jsonl",
        "schema": [
            ("date", "DATE"), ("account_id", "STRING"), ("campaign_id", "STRING"),
            ("campaign_name", "STRING"), ("objective", "STRING"), ("impressions", "INT64"),
            ("clicks", "INT64"), ("spend", "FLOAT64"),
            ("actions", [("action_type", "STRING"), ("value", "STRING")]), ("action_values", [("action_type", "STRING"), ("value", "STRING")]),
            ("_synced_at", "TIMESTAMP"),
        ],
    },
    "shopify_orders": {
        "file": DATA / "raw_shopify_orders.jsonl",
        "schema": [
            ("order_id", "STRING"), ("order_number", "STRING"), ("store_id", "STRING"),
            ("customer_id", "STRING"), ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
            ("sales_channel", "STRING"), ("financial_status", "STRING"), ("test", "BOOL"),
            ("total_discounts", "STRING"), ("total_tax", "STRING"), ("total_shipping", "STRING"),
            ("line_items", [("line_item_id", "STRING"), ("product_title", "STRING"),
                            ("quantity", "INT64"), ("price", "STRING")]),
        ],
    },
    "shopify_refunds": {
        "file": DATA / "raw_shopify_refunds.jsonl",
        "schema": [
            ("refund_id", "STRING"), ("order_id", "STRING"), ("created_at", "TIMESTAMP"),
            ("amount", "STRING"), ("reason", "STRING"),
        ],
    },
    "ga4_sessions": {
        "file": DATA / "raw_ga4_sessions.jsonl",
        "schema": [
            ("date", "DATE"), ("property_id", "STRING"), ("session_source", "STRING"),
            ("session_medium", "STRING"), ("session_campaign_name", "STRING"),
            ("sessions", "INT64"), ("engaged_sessions", "INT64"), ("new_users", "INT64"),
            ("total_users", "INT64"), ("key_events", "INT64"), ("purchase_revenue", "FLOAT64"),
            ("_synced_at", "TIMESTAMP"),
        ],
    },
}

REFERENCE_TABLES = {
    "meta_conversion_actions": REFERENCE / "meta_conversion_actions.csv",
    "ga4_channel_mapping": REFERENCE / "ga4_channel_mapping.csv",
    "order_exclusions": REFERENCE / "order_exclusions.csv",
}


# ---------------------------------------------------------------------------
# Local target (DuckDB)
# ---------------------------------------------------------------------------

class LocalTarget:
    def __init__(self):
        import duckdb
        import sqlglot
        self.sqlglot = sqlglot
        db_path = ROOT / "local.duckdb"
        db_path.unlink(missing_ok=True)
        self.con = duckdb.connect(str(db_path))
        for ds in DATASETS:
            self.con.execute(f"CREATE SCHEMA {ds}")

    def load_raw(self):
        base = {"STRING": "VARCHAR", "INT64": "BIGINT", "FLOAT64": "DOUBLE",
                "DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "BOOL": "BOOLEAN"}

        def duck_type(t):
            if isinstance(t, list):  # repeated record
                return "STRUCT(" + ", ".join(f"{n} {base[ft]}" for n, ft in t) + ")[]"
            return base[t]

        for table, spec in RAW_TABLES.items():
            cols = ", ".join(f"'{c}': '{duck_type(t)}'" for c, t in spec["schema"])
            self.con.execute(
                f"CREATE TABLE raw.{table} AS "
                f"SELECT * FROM read_json('{spec['file']}', format='newline_delimited', columns={{{cols}}})"
            )
        for table, path in REFERENCE_TABLES.items():
            self.con.execute(f"CREATE TABLE reference.{table} AS SELECT * FROM read_csv_auto('{path}')")

    def _translate(self, sql):
        return self.sqlglot.transpile(sql, read="bigquery", write="duckdb")[0]

    def build(self, dataset, name, sql):
        self.con.execute(f"CREATE OR REPLACE TABLE {dataset}.{name} AS {self._translate(sql)}")
        return self.con.execute(f"SELECT COUNT(*) FROM {dataset}.{name}").fetchone()[0]

    def query(self, sql):
        return self.con.execute(self._translate(sql)).fetchall()


# ---------------------------------------------------------------------------
# BigQuery target
# ---------------------------------------------------------------------------

class BigQueryTarget:
    def __init__(self, project, location):
        from google.cloud import bigquery
        self.bq = bigquery
        self.client = bigquery.Client(project=project, location=location)
        self.project = project
        for ds in DATASETS:
            self.client.create_dataset(f"{project}.{ds}", exists_ok=True)

    def _schema(self, spec):
        f = self.bq.SchemaField
        out = []
        for name, typ in spec:
            if isinstance(typ, list):  # repeated record
                out.append(f(name, "RECORD", mode="REPEATED",
                             fields=[f(n, t) for n, t in typ]))
            else:
                out.append(f(name, typ))
        return out

    def load_raw(self):
        for table, spec in RAW_TABLES.items():
            config = self.bq.LoadJobConfig(
                source_format=self.bq.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=self._schema(spec["schema"]),
                write_disposition="WRITE_TRUNCATE",
            )
            with open(spec["file"], "rb") as f:
                self.client.load_table_from_file(
                    f, f"{self.project}.raw.{table}", job_config=config).result()
        for table, path in REFERENCE_TABLES.items():
            config = self.bq.LoadJobConfig(
                source_format=self.bq.SourceFormat.CSV, skip_leading_rows=1,
                autodetect=True, write_disposition="WRITE_TRUNCATE",
            )
            with open(path, "rb") as f:
                self.client.load_table_from_file(
                    f, f"{self.project}.reference.{table}", job_config=config).result()

    def build(self, dataset, name, sql):
        # No date partitioning: the BigQuery sandbox expires partitions older
        # than 60 days, which would delete historical rows as soon as they're
        # written. At this data size partitioning adds nothing anyway.
        ddl = f"CREATE OR REPLACE TABLE `{self.project}.{dataset}.{name}` AS\n{sql}"
        self.client.query(ddl, job_config=self._job_config()).result()
        return self.client.get_table(f"{self.project}.{dataset}.{name}").num_rows

    def query(self, sql):
        return list(self.client.query(sql, job_config=self._job_config()).result())

    def _job_config(self):
        # Unqualified names like `staging.stg_clients` resolve to this project.
        return self.bq.QueryJobConfig(default_dataset=f"{self.project}.raw")


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["local", "bigquery"], default="local")
    parser.add_argument("--project", help="GCP project ID (bigquery target only)")
    parser.add_argument("--location", default="US")
    parser.add_argument("--skip-load", action="store_true", help="don't reload raw data")
    args = parser.parse_args()

    if args.target == "bigquery":
        if not args.project:
            sys.exit("--project is required for --target bigquery")
        target = BigQueryTarget(args.project, args.location)
    else:
        target = LocalTarget()

    if not args.skip_load or args.target == "local":
        print("Loading raw and reference data...")
        target.load_raw()

    print("\nBuilding models:")
    for dataset, name, path in MODELS:
        rows = target.build(dataset, name, path.read_text())
        print(f"  {dataset}.{name:<32} {rows:>7,} rows")

    print("\nRunning tests:")
    failures = 0
    for path in sorted(TESTS.glob("*.sql")):
        bad_rows = target.query(path.read_text())
        status = "PASS" if not bad_rows else f"FAIL ({len(bad_rows)} rows)"
        print(f"  {status:<16} {path.stem}")
        if bad_rows:
            failures += 1
            for row in bad_rows[:5]:
                print(f"      {tuple(row)}")

    print(f"\n{len(list(TESTS.glob('*.sql'))) - failures} passed, {failures} failed")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
