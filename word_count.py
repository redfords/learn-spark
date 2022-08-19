from pyspark.sql import SparkSession

spark = (SparkSession
        .builder
        .appName("Analyzing the vocabulary of Pride and Prejudice.")
        .getOrCreate())

spark.sparkContext.setLogLevel("KEYWORD")

book = spark.read.text("./data/gutenberg_books/1342-0.txt")