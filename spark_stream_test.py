from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import time


spark = SparkSession \
    .builder \
    .appName("teste") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR') 

df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "127.0.0.1:9093")
        .option("subscribe", "test")
        .option("startingOffsets", "earliest") 
        .load()
    )

df = df.selectExpr("CAST(value AS STRING)")

(
    df
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "False") 
    .start()
    .awaitTermination()
)
