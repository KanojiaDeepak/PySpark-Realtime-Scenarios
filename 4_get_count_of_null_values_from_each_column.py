from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("tryingThingsOut") \
    .getOrCreate()

path = "data/trips_2018"
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load(path)

df.printSchema()
#df.show(truncate=False)
# for c in df.columns:
#     count=df.filter(col(c).isNull()).count()
#     print(c,"-->",count)


null_count_df=df.select([count(when(col(i).isNull(),1)).alias(i) for i in df.columns])
null_count_df.show()

spark.stop()