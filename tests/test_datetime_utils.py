# test_datetime_utils.py


# Version with local spark: does not run currently, i think it has something to do with the java and python version im using.
# import os
# import sys

# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# sys.path.append(os.getcwd())

# import datetime
# from src.utils.datetime_utils import timestamp_to_date_col
# from pyspark.sql import SparkSession

# def test_timestamp_to_date_col():
            
#     # spark = SparkSession.builder.getOrCreate()
#     spark = (SparkSession.builder
#     # Turn off PyArrow to prevent Windows/Python 3.12 serialization crashes
#     .config("spark.sql.execution.arrow.pyspark.enabled", "false")
#     # Enable the fault handler so if it crashes again, it prints WHY it crashed
#     .config("spark.python.worker.faulthandler.enabled", "true")
#     .getOrCreate())
    
#     # Create a DataFrame with a known timestamp column using a datetime object
#     data = [(datetime.datetime(2025, 4, 10, 10, 30, 0),)]
#     schema = "ride_timestamp timestamp"
#     df = spark.createDataFrame(data, schema=schema)
    
#     # Use the utility to add a date column
#     result_df = timestamp_to_date_col(spark, df, "ride_timestamp", "ride_date")
    
#     # Assert that the extracted date matches the expected value
#     row = result_df.select("ride_date").first()

#     expected_date = datetime.date(2025, 4, 10)  # Expected: 2025-04-10

#     assert row["ride_date"] == expected_date


#########################################################################################################
# Databricks Connect Version

import datetime
from src.utils.datetime_utils import timestamp_to_date_col



def test_timestamp_to_date_col(spark):
    # Create a DataFrame with a known timestamp column using a datetime object
    data = [(datetime.datetime(2025, 4, 10, 10, 30, 0),)]
    schema = "ride_timestamp timestamp"
    df = spark.createDataFrame(data, schema=schema)
    
    # Use the utility to add a date column
    result_df = timestamp_to_date_col(spark, df, "ride_timestamp", "ride_date")
    
    # Assert that the extracted date matches the expected value
    row = result_df.select("ride_date").first()

    expected_date = datetime.date(2025, 4, 10)  # Expected: 2025-04-10

    assert row["ride_date"] == expected_date