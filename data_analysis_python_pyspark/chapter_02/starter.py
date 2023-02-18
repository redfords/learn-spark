from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("Analyzing the vocabulary of Pride and Prejudice.")
    .getOrCreate())

# read from Paradise Lost, by John Milton

book = spark.read.text("20-0.txt")