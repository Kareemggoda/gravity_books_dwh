# 02_transform/transform_dims.py

import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, "01_extract"))
sys.path.append(os.path.join(project_root, "02_transform"))

from config.spark_session import get_spark_session
from config.config import JDBC_URL, JDBC_DRIVER, DB_USER, DB_PASSWORD
from extract import extract_all

from dim_customer     import build_dim_customer
from dim_book         import build_dim_book
from dim_date         import build_dim_date
from dim_shipping     import build_dim_shipping
from dim_order_status import build_dim_order_status
from fact_order_sales import build_fact_order_sales


def build_all_dims(dataframes):

    print("=" * 50)
    print("BUILDING DIMENSION TABLES")
    print("=" * 50)

    dims = {}

    dims["dim_customer"] = build_dim_customer(dataframes)
    print(f"✅ dim_customer     → {dims['dim_customer'].count():>6} rows")

    dims["dim_book"] = build_dim_book(dataframes)
    print(f"✅ dim_book         → {dims['dim_book'].count():>6} rows")

    dims["dim_date"] = build_dim_date(dataframes)
    print(f"✅ dim_date         → {dims['dim_date'].count():>6} rows")

    dims["dim_shipping"] = build_dim_shipping(dataframes)
    print(f"✅ dim_shipping     → {dims['dim_shipping'].count():>6} rows")

    dims["dim_order_status"] = build_dim_order_status(dataframes)
    print(f"✅ dim_order_status → {dims['dim_order_status'].count():>6} rows")

    print("=" * 50)
    print(f"✅ All {len(dims)} dimension tables built!")
    print("=" * 50)

    return dims


def build_fact(dataframes, dims):

    print("\n" + "=" * 50)
    print("BUILDING FACT TABLE")
    print("=" * 50)

    fact = build_fact_order_sales(dataframes, dims)
    print(f"✅ fact_order_sales → {fact.count():>6} rows")

    print("=" * 50)
    print("✅ Fact table built!")
    print("=" * 50)

    return fact


def validate_all(dims, fact):

    print("\n" + "=" * 50)
    print("VALIDATION")
    print("=" * 50)

    all_tables = {**dims, "fact_order_sales": fact}

    for name, df in all_tables.items():
        print(f"\n── {name.upper()} ──")
        df.printSchema()
        df.show(3, truncate=True)


if __name__ == "__main__":
    spark = get_spark_session()

    try:
        # Step 1: Extract
        dataframes = extract_all(spark)

        # Step 2: Build dims
        dims = build_all_dims(dataframes)

        # Step 3: Build fact
        fact = build_fact(dataframes, dims)

        # Step 4: Validate
        validate_all(dims, fact)

    finally:
        spark.stop()
        print("\n✅ Spark session stopped.")