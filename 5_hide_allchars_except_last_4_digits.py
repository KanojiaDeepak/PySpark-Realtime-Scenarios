from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("tryingThingsOut") \
    .getOrCreate()

path = "data/credit_card_data.csv"
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load(path)

df.printSchema()
df_final= df.select("id","name",col("creditcardnumber").cast("string"))
df_final= df.withColumn("creditcardnumber",concat(lit("********"),substring("creditcardnumber",9,4)))
df_final.show()

spark.stop()