from pyspark.sql import SparkSession

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

# print(book)
# DataFrame[value: string]

# book.printSchema()
# root
#  |-- value: string (nullable = true)

# print(book.dtypes)
# [('value', 'string')]

book.show(10, truncate=50)