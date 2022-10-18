from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import numpy as np
import os

from sqlalchemy import column

spark = SparkSession.builder.getOrCreate()

DIRECTORY = "./data/broadcast_logs"

logs = spark.read.csv(
    path=os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

logs.printSchema()

# Selecting five rows of the first three columns of our data frame
logs.select("BroadcastLogID", "LogServiceID", "LogDate").show(5, False)

# Four ways to select columns in PySpark, all equivalent in terms of results
# Using the string to column conversion
logs.select("BroadCastLogID", "LogServiceID", "LogDate")
logs.select(*["BroadCastLogID", "LogServiceID", "LogDate"])

# Passing the column object explicitly
logs.select(
    F.col("BroadCastLogID"), F.col("LogServiceID"), F.col("LogDate")
)
logs.select(
    *[F.col("BroadCastLogID"), F.col("LogServiceID"), F.col("LogDate")]
)

# Split an array into multiple sub-arrays

column_split = np.array_split(
    np.array(logs.columns), len(logs.columns)
)

print(column_split)

for x in column_split:
    logs.select(*x).show(5, False)

# Deleting columns
logs = logs.drop("BroadcastLogID", "SequenceNO")

# Testing if we effectively got rid of the columns
print("BroadcastLogID" in logs.columns) # => False
print("SequenceNo" in logs.columns) # => False