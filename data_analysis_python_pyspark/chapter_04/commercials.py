from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.getOrCreate()

DIRECTORY = "./data/broadcast_logs"

logs = spark.read.csv(
    path=os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)