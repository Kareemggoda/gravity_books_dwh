# 02_transform/dim_shipping.py

from pyspark.sql import functions as F

def build_dim_shipping(dataframes):
    """
    Build dim_shipping from shipping_method table.
    """
    shipping = dataframes["shipping_method"]

    dim_shipping = shipping \
        .select(
            F.col("method_id").alias("shipping_key"),
            F.col("method_id"),
            F.col("method_name"),
            F.col("cost")
        )

    return dim_shipping