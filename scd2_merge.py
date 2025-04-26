from delta.tables import DeltaTable

booking_data_loc = "/Volumes/booking_scd2_merge/default/booking_volume/booking_data/"
customer_data_loc = "/Volumes/booking_scd2_merge/default/booking_volume/customer_data/"

booking_fact_table_name = "booking_scd2_merge.default.booking_fact"
customer_dim_table_name = "booking_scd2_merge.default.customer_dim"


from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, expr

def merge_customers_dim(spark, customers_df):
    # Check if the Delta table exists
    dim_table_exists = spark._jsparkSession.catalog().tableExists(customer_dim_table_name)

    if dim_table_exists:
        customer_dim_table = DeltaTable.forName(spark, customer_dim_table_name)
        

        # Merge Logic
        customer_dim_table.alias("target").merge(
            customers_df.alias("source"),
            "target.customer_id = source.customer_id AND target.valid_to = '9999-12-31'"
        ).whenMatchedUpdate(set={
                "valid_to": "source.valid_from"
            }
        ).whenNotMatchedInsert(values={
            "customer_id": "source.customer_id",
            "customer_name": "source.customer_name",
            "customer_address": "source.customer_address",
            "phone_number": "source.phone_number",
            "email": "source.email",
            "valid_from": "source.valid_from",
            "valid_to": "'9999-12-31'"
        }).execute()

    else:
        customers_df.write.format("delta").mode("overwrite").saveAsTable(customer_dim_table_name)
    
    print(f"Customer dimension table {customer_dim_table_name} updated successfully")



