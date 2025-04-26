from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

def run_dq_checks(spark, bookings_df, customers_df):
    # Define Data Quality Checks for Bookings
    check_booking = Check(spark, CheckLevel.Error, "DQ Checks") \
        .isComplete("booking_id") \
        .isUnique("booking_id") \
        .isComplete("customer_id") \
        .hasMin("amount", lambda x: x >= 0)

    # Define Data Quality Checks for Customers
    check_customer = Check(spark, CheckLevel.Error, "DQ Checks") \
        .isComplete("customer_id")
    
    # Run Verification
    result_booking = VerificationSuite(spark).onData(bookings_df).addCheck(check_booking).run()
    result_customer = VerificationSuite(spark).onData(customers_df).addCheck(check_customer).run()

    # ⚡ Correct way to convert results to DataFrame
    booking_check_result_df = VerificationResult.checkResultsAsDataFrame(spark, result_booking).collect()
    customer_check_result_df = VerificationResult.checkResultsAsDataFrame(spark, result_customer).collect()

    return booking_check_result_df, customer_check_result_df



