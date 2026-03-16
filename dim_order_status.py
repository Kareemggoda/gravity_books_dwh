# 02_transform/dim_order_status.py

from pyspark.sql import functions as F

def build_dim_order_status(dataframes):
    """
    Build dim_order_status from order_status table.
    """
    order_status = dataframes["order_status"]

    dim_order_status = order_status \
        .select(
            F.monotonically_increasing_id().alias("status_key"),
            F.col("status_id"),
            F.col("status_value")
        )

    return dim_order_status