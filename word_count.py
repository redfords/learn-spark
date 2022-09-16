from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lower, split, regexp_extract

spark = (SparkSession
        .builder
        .appName("Analyzing the vocabulary of Pride and Prejudice.")
        .config("spark.sql.repl.eagerEval.enabled", "True")
        .getOrCreate())

# log-level keywords
# off, fatal, error, warn, info, debug, trace, all

spark.sparkContext.setLogLevel("ERROR")

# mapping our program
# 1. read the input data
# 2. tokenize each word
# 3. clean and remove punctuaton or non words, lowercase each word
# 4. count the frequency of each word
# 5. return the top 10 (or 20, 50, 100)

book = spark.read.text("./data/gutenberg_books/1342-0.txt")

print(book)
# DataFrame[value: string]

book.printSchema()
# root
#  |-- value: string (nullable = true)

print(book.dtypes)
# [('value', 'string')]

book.show(10, truncate=50)
book.show(10, truncate=False, vertical=True)

# simple column transformation

lines = book.select(split(book.value, " ").alias("line"))
lines.printSchema()
lines.show(5)

# all sparksql functions
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

# the simplest select statement
book.select(book.value)
book.select(book["value"])

# the col function doesn't specify the the column comes from the book data frame
book.select(col("value"))
book.select("value")

# split can use regular expression instead of a space, and an int about how many times
# we apply the delimiter
# lines = book.select(split(col("value"), " "))

lines.printSchema()
# root
#  |-- line: array (nullable = true)
#  |    |-- element: string (containsNull = true)

# use .withColumnRenamed() to rename a column without changing the df

# explode list into words
words = lines.select(explode(col("line")).alias("word"))
words.show(15)

# change case and remove punctuation
words_lower = words.select(lower(col("word")).alias("word_lower"))
words_lower.show()

# use regex to keep only letters a-z
words_clean = words_lower.select(
        regexp_extract(col("word_lower"), "[a-z]+", 0).alias("word")
)
words_clean.show()