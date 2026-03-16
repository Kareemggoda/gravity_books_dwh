# 02_transform/dim_date.py

from pyspark.sql import functions as F

def build_dim_date(dataframes):
    """
    Build dim_date from distinct order dates in cust_order.
    """
    cust_order = dataframes["cust_order"]

    dim_date = cust_order \
        .select(F.col("order_date").cast("date").alias("full_date")) \
        .dropDuplicates() \
        .filter(F.col("full_date").isNotNull()) \
        .select(
            F.monotonically_increasing_id().alias("date_key"),
            F.col("full_date"),
            F.year("full_date").alias("year"),
            F.month("full_date").alias("month"),
            F.dayofmonth("full_date").alias("day"),
            F.quarter("full_date").alias("quarter"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.date_format("full_date", "MMMM").alias("month_name")
        )

    return dim_date