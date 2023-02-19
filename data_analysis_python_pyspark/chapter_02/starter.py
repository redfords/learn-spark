from pyspark.sql import SparkSession
import os

spark = (SparkSession
    .builder
    .appName("Analyzing the vocabulary of Pride and Prejudice.")
    .getOrCreate())

# read from Paradise Lost, by John Milton

base_dir = os.path.abspath(os.getcwd())

book = spark.read.text(os.path.join(base_dir, "20-0.txt"))
book.printSchema()
print(book.dtypes)

# n (n of rows, default 20)
# truncate (truncate strings longer than 20 chars, default True)
# vertical (print rows vertically, default False)
book.show(10, False, True)