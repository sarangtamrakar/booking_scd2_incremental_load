from pyspark.sql import SparkSession
from utils import get_input_path, should_continue
# from dq_checks import run_dq_checks

from dq_checks import run_dq_checks
from agg_merge import merge_bookings_fact
from scd2_merge import merge_customers_dim
from config import booking_data_loc ,customer_data_loc
import sys
from datetime import datetime




if __name__ == "__main__":
    spark = SparkSession.builder.appName("TravelBookingsPipeline").getOrCreate()
    
    # Take input_date from Databricks Workflow parameter
    try:
        input_date = dbutils.widgets.get("input_date")
    except Exception as e:
        # Fallback for local manual runs (optional)
        input_date = "2025-04-27"   # <-- Default or testing value

    # Step 2: Validate the format
    try:
        # This will raise ValueError if format is wrong
        datetime.strptime(input_date, "%Y-%m-%d")
        print(f"Running pipeline for date: {input_date}")
    except ValueError:
        raise ValueError(f"Provided input_date '{input_date}' is not in 'YYYY-MM-DD' format. Please fix the date.")




    # Load data
    bookings_df = spark.read.csv(get_input_path(booking_data_loc, input_date,"bookings"), header=True, inferSchema=True)

    customers_df = spark.read.csv(get_input_path(customer_data_loc, input_date,"customers"), header=True, inferSchema=True)

    # Data Quality Checks
    booking_dq_pass , customer_dq_pass = run_dq_checks(spark, bookings_df, customers_df)

    print(booking_dq_pass)

    if not (should_continue(booking_dq_pass) and should_continue(customer_dq_pass)):
        raise Exception("Data Quality checks failed! Stopping the pipeline ❌")
    else:
        print("All Data Quality checks passed ✅. Continuing...")
    

    print("✅ Data Quality checks passed. Proceeding with transformations...")

    # Merge bookings_fact
    merge_bookings_fact(spark, bookings_df)

    # Merge customers_dim using SCD2
    merge_customers_dim(spark, customers_df)

    print("✅ All transformations completed")
    print(f"Pipeline run successfully for date {input_date}")


