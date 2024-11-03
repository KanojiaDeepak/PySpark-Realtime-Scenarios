from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("tryingThingsOut") \
    .getOrCreate()

df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("data/trips_2018")

df.printSchema()

num_partitions = df.rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

df=df.withColumn("filename",input_file_name())
df.groupBy("filename").count().show(truncate=False)

spark.stop()


