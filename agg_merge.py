from pyspark.sql.functions import col, lit, current_timestamp, sum as _sum
from delta.tables import DeltaTable


booking_data_loc = "/Volumes/booking_scd2_merge/default/booking_volume/booking_data/"
customer_data_loc = "/Volumes/booking_scd2_merge/default/booking_volume/customer_data/"

booking_fact_table_name = "booking_scd2_merge.default.booking_fact"
customer_dim_table_name = "booking_scd2_merge.default.customer_dim"

def merge_bookings_fact(spark, df):

    df_transformed = df.withColumn("total_cost", col("amount") - col("discount"))

    # Group by and aggregate df_transformed
    df_transformed_agg = df_transformed \
        .groupBy("booking_type", "customer_id") \
        .agg(
            _sum("total_cost").alias("total_amount_sum"),
            _sum("quantity").alias("total_quantity_sum")
        )

    # Check if the Delta table exists
    fact_table_exists = spark._jsparkSession.catalog().tableExists(booking_fact_table_name)

    if fact_table_exists:
        
        booking_fact_table = DeltaTable.forName(spark, booking_fact_table_name)

        booking_fact_table.alias("target").merge(
        df_transformed_agg.alias("source"),
        "target.customer_id = source.customer_id AND target.booking_type = source.booking_type").whenMatchedUpdate(set={
        "total_amount_sum": "target.total_amount_sum + source.total_amount_sum",
        "total_quantity_sum": "target.total_quantity_sum + source.total_quantity_sum"}).whenNotMatchedInsertAll().execute()
    else:
        df_transformed_agg.write.format("delta").mode("overwrite").saveAsTable(booking_fact_table_name)
    
    print(f"added the booking agg fact data to {booking_fact_table_name}")

    



