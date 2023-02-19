from pyspark.sql import SparkSession
import os

spark = (SparkSession
    .builder
    .appName("Analyzing the vocabulary of Pride and Prejudice.")
    .getOrCreate())

# read from Paradise Lost, by John Milton

print("File location using os.getcwd():", os.getcwd())
 
print(f"File location using __file__ variable: {os.path.realpath(os.path.dirname(__file__))}")

BASE_DIR = "data_analysis_python_pyspark"
# book = spark.read.text("../../20-0.txt")
# book.printSchema()
