# 01_extract/extract.py
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.spark_session import get_spark_session
from config.config import JDBC_URL, JDBC_DRIVER, DB_USER, DB_PASSWORD

def extract_table(spark, table_name):
    """Extract a single table from SQL Server into a PySpark DataFrame."""
    df = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", f"dbo.{table_name}") \
        .option("driver", JDBC_DRIVER) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .load()
    return df

def extract_all(spark): 
    """Extract all 11 source tables."""
    
    tables = [
        # Book group
        "book",
        "author",
        "book_author",
        "book_language",
        "publisher",
        # Order group
        "cust_order",
        "order_line",
        "order_history",
        "order_status",
        "shipping_method",
        # Customer group
        "customer",
        "address",
        "customer_address",
        "country",
        "address_status"
    ]
    
    dataframes = {}
    
    print("=" * 50)
    print("EXTRACTION STARTED")
    print("=" * 50)
    
    for table in tables:
        try:
            df = extract_table(spark, table)
            count = df.count()
            dataframes[table] = df
            print(f"{table:<25} → {count:>6} rows")
        except Exception as e:
            print(f"{table:<25} → FAILED: {e}")
    
    print("=" * 50)
    print(f"Extracted {len(dataframes)}/{len(tables)} tables")
    print("=" * 50)
    
    return dataframes

def validate_extract(dataframes):
    """Print schema and sample rows for each table."""
    
    print("\n" + "=" * 50)
    print("VALIDATION - SCHEMAS & SAMPLES")
    print("=" * 50)
    
    for table_name, df in dataframes.items():
        print(f"\n── {table_name.upper()} ──")
        df.printSchema()
        df.show(3, truncate=True)

if __name__ == "__main__":
    spark = get_spark_session()
    
    try:
        # Extract all tables
        dataframes = extract_all(spark)
        
        # Validate extraction
        validate_extract(dataframes)
        
    finally:
        spark.stop()
        print("\nSpark session stopped.")
