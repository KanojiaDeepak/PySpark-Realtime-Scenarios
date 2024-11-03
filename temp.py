from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Sample PySpark Code") \
    .getOrCreate()

spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

df = spark.read.format("csv").option("header",True).option("inferSchema",True).load(r"C:\Users\kanoj\Downloads\Divvy_Trips_2018_Q4\Divvy_Trips_2018_Q4.csv")

df.show(5, truncate=False)

df.printSchema()

num_partitions = df.rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

df.write.format("csv").option("header",True).mode("overwrite").save("data/trips_2018/")

spark.stop()


