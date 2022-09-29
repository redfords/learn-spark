from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Before
# book = spark.read.text("./data/gutenberg_books/1342-0.txt")
# lines = book.select(split(book.value, " ").alias("line"))
# words = lines.select(explode(col("line")).alias("word"))
# words_lower = words.select(lower(col("word")).alias("word"))
# words_clean = words_lower.select(
#         regexp_extract(col("word"), "[a-z']*", 0).alias("word")
# )
# words_nonull = words_clean.where(col("word") != "")
# results = words_nonull.groupby("word").count()

# After
spark = SparkSession.builder.appName(
    "Counting word occurences from a book."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# If you need to read multiple text files, replace `1342-0` by `*`.
# Since we are performing two actions on results (displaying the top 10 words
# and writing the df to a csv file) we use a variable. 

results = (
    spark.read.text("../../data/gutenberg_books/1342-0.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
)

results.orderBy("count", ascending=False).show(10)
results.coalesce(1).write.csv("./results_single_partition.csv")