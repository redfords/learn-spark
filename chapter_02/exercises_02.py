from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, greatest

spark = SparkSession.builder.getOrCreate()

# count the number of columns that aren't strings

exo2_2_df = spark.createDataFrame(
    [["test", "more test", 10_000_000_000]], ["one", "two", "three"]
)

# rewrite without the 

exo2_3_df = (
    spark.read.text("./data/gutenberg_books/1342-0.txt")
    .select(length(col("value")))
    .withColumnRenamed("length(value)", "number_of_char")
)

# return the greatest value using select and greatest

exo2_4_df = spark.createDataFrame(
    [["key", 10_000, 20_000]], ["key", "value1", "value2"]
)

# remove all the occurrences of the words is
# keep only the words with more than three characters using the length function
# remove various words using the isin() method
