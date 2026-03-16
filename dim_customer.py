# 02_transform/dim_customer.py

from pyspark.sql import functions as F

def build_dim_customer(dataframes):

    customer         = dataframes["customer"]
    customer_address = dataframes["customer_address"]
    address          = dataframes["address"]
    country          = dataframes["country"]

    active_address = customer_address.filter(F.col("status_id") == 1)

    dim_customer = customer \
        .join(active_address, "customer_id", "left") \
        .join(address, "address_id", "left") \
        .join(country, "country_id", "left") \
        .select(
            F.col("customer_id").alias("customer_key"),  # ← use customer_id directly
            F.col("customer_id"),
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name"),
            F.col("email"),
            F.col("street_number"),
            F.col("street_name"),
            F.col("city"),
            F.col("country_name").alias("country")
        ) \
        .dropDuplicates(["customer_key"]) \
        .fillna("Unknown")

    return dim_customer