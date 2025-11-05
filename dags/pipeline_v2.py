from __future__ import annotations

import csv
import os
import re
import shutil
from datetime import datetime, timedelta

from faker import Faker

from airflow import DAG
from airflow.sdk import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import Error as DatabaseError
from psycopg2 import sql

OUTPUT_ROOT = "/opt/airflow/data"
RAW_DIR = os.path.join(OUTPUT_ROOT, "raw")
CLEAN_DIR = os.path.join(OUTPUT_ROOT, "clean")
ANALYSIS_DIR = os.path.join(OUTPUT_ROOT, "output")

for d in (RAW_DIR, CLEAN_DIR, ANALYSIS_DIR):
    os.makedirs(d, exist_ok=True)

PG_CONN_ID = "Postgres"
PG_SCHEMA = "week8_demo"
TARGET_TABLE = "employees"

default_args = {"owner": "IDS706", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="pipeline_v2",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    description="ETL: ingestion → transform → merge → load → analyze → cleanup",
) as dag:

    @task()
    def fetch_persons(output_dir: str = RAW_DIR, quantity: int = 100) -> str:
        fake = Faker()
        data = []
        for _ in range(quantity):
            data.append(
                {
                    "firstname": fake.first_name(),
                    "lastname": fake.last_name(),
                    "email": fake.free_email(),
                    "phone": fake.phone_number(),
                    "address": fake.street_address(),
                    "city": fake.city(),
                    "country": fake.country(),
                }
            )
        filepath = os.path.join(output_dir, "persons.csv")
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        return filepath

    @task()
    def fetch_companies(output_dir: str = RAW_DIR, quantity: int = 100) -> str:
        fake = Faker()
        data = []
        for _ in range(quantity):
            data.append(
                {
                    "name": fake.company(),
                    "email": f"info@{fake.domain_name()}",
                    "phone": fake.phone_number(),
                    "country": fake.country(),
                    "website": fake.url(),
                    "industry": fake.bs().split()[0].capitalize(),
                    "catch_phrase": fake.catch_phrase(),
                    "employees_count": fake.random_int(min=10, max=5000),
                    "founded_year": fake.year(),
                }
            )
        filepath = os.path.join(output_dir, "companies.csv")
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        return filepath

    with TaskGroup(group_id="ingest") as ingest:
        persons_raw = fetch_persons()
        companies_raw = fetch_companies()

    @task()
    def transform_persons(persons_path: str, out_dir: str = CLEAN_DIR) -> str:
        out_path = os.path.join(out_dir, "persons_clean.csv")
        with open(persons_path, newline="", encoding="utf-8") as f_in, open(
            out_path, "w", newline="", encoding="utf-8"
        ) as f_out:
            r = csv.DictReader(f_in)
            cols = [
                "firstname",
                "lastname",
                "email",
                "phone",
                "address",
                "city",
                "country",
            ]
            w = csv.DictWriter(f_out, fieldnames=cols)
            w.writeheader()
            for row in r:
                email = (row.get("email", "") or "").strip().lower()
                if "@" not in email:
                    continue
                phone = re.sub(r"\\D+", "", row.get("phone", "") or "")
                w.writerow(
                    {
                        "firstname": (row.get("firstname", "") or "").strip().title(),
                        "lastname": (row.get("lastname", "") or "").strip().title(),
                        "email": email,
                        "phone": phone,
                        "address": (row.get("address", "") or "").strip(),
                        "city": (row.get("city", "") or "").strip().title(),
                        "country": (row.get("country", "") or "").strip().title(),
                    }
                )
        return out_path

    @task()
    def transform_companies(companies_path: str, out_dir: str = CLEAN_DIR) -> str:
        out_path = os.path.join(out_dir, "companies_clean.csv")
        with open(companies_path, newline="", encoding="utf-8") as f_in, open(
            out_path, "w", newline="", encoding="utf-8"
        ) as f_out:
            r = csv.DictReader(f_in)
            cols = [
                "name",
                "email",
                "phone",
                "country",
                "website",
                "industry",
                "catch_phrase",
                "employees_count",
                "founded_year",
            ]
            w = csv.DictWriter(f_out, fieldnames=cols)
            w.writeheader()
            for row in r:
                employees = row.get("employees_count", "")
                founded = row.get("founded_year", "")
                w.writerow(
                    {
                        "name": (row.get("name", "") or "").strip(),
                        "email": (row.get("email", "") or "").strip().lower(),
                        "phone": (row.get("phone", "") or "").strip(),
                        "country": (row.get("country", "") or "").strip().title(),
                        "website": (row.get("website", "") or "").strip(),
                        "industry": (row.get("industry", "") or "").strip().title(),
                        "catch_phrase": (row.get("catch_phrase", "") or "").strip(),
                        "employees_count": employees,
                        "founded_year": founded,
                    }
                )
        return out_path

    with TaskGroup(group_id="transform") as transform:
        persons_clean = transform_persons(persons_raw)
        companies_clean = transform_companies(companies_raw)

    @task()
    def merge_csvs(
        persons_path: str, companies_path: str, output_dir: str = CLEAN_DIR
    ) -> str:
        merged_path = os.path.join(output_dir, "merged_data.csv")
        with open(persons_path, newline="", encoding="utf-8") as f1, open(
            companies_path, newline="", encoding="utf-8"
        ) as f2:
            persons = list(csv.DictReader(f1))
            companies = list(csv.DictReader(f2))
        merged = []
        n = min(len(persons), len(companies))
        for i in range(n):
            p, c = persons[i], companies[i]
            merged.append(
                {
                    "firstname": p["firstname"],
                    "lastname": p["lastname"],
                    "email": p["email"],
                    "person_country": p["country"],
                    "company_name": c["name"],
                    "company_email": c["email"],
                    "company_country": c["country"],
                    "industry": c["industry"],
                    "employees_count": c["employees_count"],
                    "founded_year": c["founded_year"],
                }
            )
        with open(merged_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=merged[0].keys())
            writer.writeheader()
            writer.writerows(merged)
        return merged_path

    merged_path = merge_csvs(persons_clean, companies_clean)

    @task()
    def load_csv_to_pg(
        conn_id: str,
        csv_path: str,
        schema: str = PG_SCHEMA,
        table: str = TARGET_TABLE,
        append: bool = True,
    ) -> int:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = [
                tuple((r.get(col, "") or None) for col in fieldnames) for r in reader
            ]
        if not rows:
            return 0
        create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        # Safer approach: if the table doesn't exist, create it using the
        # CSV fieldnames and reasonable column types; if it does exist,
        # ALTER TABLE to add any missing columns (no DROP). If append is
        # False, delete existing rows instead of dropping the table.
        delete_rows = f"DELETE FROM {schema}.{table};" if not append else None
        insert_sql = f"INSERT INTO {schema}.{table} ({', '.join(fieldnames)}) VALUES ({', '.join(['%s' for _ in fieldnames])});"
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_schema)

                # Decide desired column types heuristically
                desired_cols: dict[str, str] = {}
                for col in fieldnames:
                    if col in ("employees_count", "founded_year"):
                        desired_cols[col] = "INT"
                    else:
                        desired_cols[col] = "TEXT"

                # Check whether the table exists
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s);",
                    (schema, table),
                )
                exists = cur.fetchone()[0]

                if not exists:
                    # Build CREATE TABLE statement with desired columns
                    cols_sql = sql.SQL(", ").join(
                        sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t))
                        for c, t in desired_cols.items()
                    )
                    cur.execute(
                        sql.SQL("CREATE TABLE {}.{} ({})").format(
                            sql.Identifier(schema), sql.Identifier(table), cols_sql
                        )
                    )
                else:
                    # Add any missing columns
                    cur.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s;",
                        (schema, table),
                    )
                    existing = {r[0] for r in cur.fetchall()}
                    for c, t in desired_cols.items():
                        if c not in existing:
                            cur.execute(
                                sql.SQL(
                                    "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {};"
                                ).format(
                                    sql.Identifier(schema),
                                    sql.Identifier(table),
                                    sql.Identifier(c),
                                    sql.SQL(t),
                                )
                            )

                if delete_rows:
                    cur.execute(delete_rows)

                cur.executemany(insert_sql, rows)
                conn.commit()
            return len(rows)
        except DatabaseError as e:
            print(f"Database error: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    inserted_rows = load_csv_to_pg(
        conn_id=PG_CONN_ID, csv_path=merged_path, schema=PG_SCHEMA, table=TARGET_TABLE
    )

    @task()
    def analyze_from_db(
        conn_id: str,
        schema: str = PG_SCHEMA,
        table: str = TARGET_TABLE,
        out_dir: str = ANALYSIS_DIR,
    ) -> str:
        os.makedirs(out_dir, exist_ok=True)
        hook = PostgresHook(postgres_conn_id=conn_id)
        # Query the loaded table using the schema/table passed into the task
        # and the expected column names produced by the merge step.
        sql = f"""
            SELECT
                company_country AS country,
                COUNT(*) AS num_records,
                COUNT(DISTINCT company_name) AS num_companies
            FROM {schema}.{table}
            GROUP BY company_country
            ORDER BY num_records DESC;
        """
        rows = hook.get_records(sql)
        csv_path = os.path.join(out_dir, "country_summary.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["country", "num_records", "num_companies"])
            w.writerows(rows)
        try:
            import matplotlib.pyplot as plt

            countries = [r[0] for r in rows[:10]]
            counts = [int(r[1]) for r in rows[:10]]
            plt.figure(figsize=(9, 4))
            plt.bar(countries, counts)
            plt.title("Top Countries by People Records")
            plt.xticks(rotation=30, ha="right")
            plt.tight_layout()
            plt.savefig(os.path.join(out_dir, "top_countries.png"))
            plt.close()
        except Exception as e:
            print(f"Plot skipped: {e}")
        return csv_path

    analysis_out = analyze_from_db(
        conn_id=PG_CONN_ID, schema=PG_SCHEMA, table=TARGET_TABLE
    )

    @task()
    def clear_intermediate(raw_dir: str = RAW_DIR, clean_dir: str = CLEAN_DIR) -> None:
        for folder_path in (raw_dir, clean_dir):
            if not os.path.exists(folder_path):
                continue
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.remove(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f"Failed to delete {file_path}: {e}")

    cleanup = clear_intermediate()

    ingest >> transform >> merged_path >> inserted_rows >> analysis_out >> cleanup
