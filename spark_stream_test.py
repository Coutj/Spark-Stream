from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

@udf
def add_word(w):
    return f"Word {w}"


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
df = df.select(add_word("value"))

(
    df
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "False") 
    .start()
    .awaitTermination()
)
