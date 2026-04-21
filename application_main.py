import sys
from lib import dataManupilation, dataReader, Utils


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]
    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)
    print("Created Spark Session")

    orders_df = dataReader.read_orders(spark, job_run_env)
    orders_filtered = dataManupilation.filter_closed_orders(orders_df)

    customers_df = dataReader.read_customers(spark, job_run_env)
    joined_df = dataManupilation.join_orders_customers(orders_filtered, customers_df)

    aggregated_results = dataManupilation.count_orders_state(joined_df)
    aggregated_results.show()
    print("end of main")

# while running specify the environment as argument, for example:
# spark-submit --master local[2] application_Main.py LOCAL
