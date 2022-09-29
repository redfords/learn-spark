from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

my_grocery_list = [
    ["Banana", 2, 1.74],
    ["Apple", 4, 2.04],
    ["Carrot", 1, 1.09],
    ["Cake", 1, 10.99],
]