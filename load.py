# 03_load/load.py

import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, "01_extract"))
sys.path.append(os.path.join(project_root, "02_transform"))

from config.spark_session import get_spark_session
from config.config import DWH_JDBC_URL, JDBC_DRIVER, DB_USER, DB_PASSWORD
from extract import extract_all
from transform_dims import build_all_dims, build_fact


def load_table(df, table_name):
    """Write a single DataFrame to SQL Server DWH."""
    try:
        df.write \
            .format("jdbc") \
            .option("url", DWH_JDBC_URL) \
            .option("dbtable", f"dbo.{table_name}") \
            .option("driver", JDBC_DRIVER) \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("batchsize", 1000) \
            .mode("append") \
            .save()
        print(f"✅ {table_name:<25} → loaded successfully")
    except Exception as e:
        print(f"❌ {table_name:<25} → FAILED: {e}")


def load_all(dims, fact):
    """Load all dimension tables then fact table into DWH."""

    print("=" * 50)
    print("LOADING INTO gravity_books_dwh")
    print("=" * 50)

    # Load dimensions first (fact depends on them)
    for table_name, df in dims.items():
        load_table(df, table_name)

    # Load fact table last
    load_table(fact, "fact_order_sales")

    print("=" * 50)
    print("✅ All tables loaded into SQL Server!")
    print("=" * 50)


def verify_load():
    """Verify row counts in DWH match PySpark counts."""

    import pyodbc

    print("\n" + "=" * 50)
    print("VERIFICATION - ROW COUNTS IN DWH")
    print("=" * 50)

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=gravity_books_dwh;"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD}"
    )

    tables = [
        "dim_customer",
        "dim_book",
        "dim_date",
        "dim_shipping",
        "dim_order_status",
        "fact_order_sales"
    ]

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM dbo.{table}")
            count = cursor.fetchone()[0]
            print(f"✅ {table:<25} → {count:>6} rows in SQL Server")

        conn.close()

    except Exception as e:
        print(f"❌ Verification failed: {e}")
        print("→ Check ODBC Driver is installed")


if __name__ == "__main__":
    spark = get_spark_session()

    try:
        # Step 1: Extract
        dataframes = extract_all(spark)

        # Step 2: Build dims
        dims = build_all_dims(dataframes)

        # Step 3: Build fact
        fact = build_fact(dataframes, dims)

        # Step 4: Load into SQL Server
        load_all(dims, fact)

        # Step 5: Verify
        verify_load()

    finally:
        spark.stop()
        print("\n✅ Spark session stopped.")