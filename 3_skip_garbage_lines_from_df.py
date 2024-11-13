from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("tryingThingsOut") \
    .getOrCreate()

#first 2 lines are garbage and we want to skip it
path="data/garbage_lines_data.csv"

sc = spark.sparkContext
rdd = sc.textFile(path)
rdd = rdd.zipWithIndex().filter(lambda x:x[1]>1).map(lambda x:x[0].split(","))

#if nextline is header now, use it or else directly convert rdd to df
header = rdd.first()

df = rdd.filter(lambda x:x!=header).toDF(header)
df.show(truncate=False)

spark.stop()
