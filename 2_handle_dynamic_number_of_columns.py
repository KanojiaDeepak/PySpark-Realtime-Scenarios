from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("tryingThingsOut") \
    .getOrCreate()


#if there are no headers in CSV and we have dynamic number of columns,
#read it as text format and then use split function to do create columns dynamically
path = "data/dynamic_cols_data.csv"
df = spark.read.format("text").load(path)

df = df.withColumn("split_col",split("value",",")).drop("value")
no_of_columns=df.select(max(size("split_col"))).collect()[0][0]

for i in range(no_of_columns):
    df = df.withColumn(f"col{i}",col("split_col")[i])

df.show(truncate=False)

spark.stop()


