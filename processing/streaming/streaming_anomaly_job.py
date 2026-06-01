from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, expr, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, MapType
import requests
import logging

# -------------------------------
# 1. Spark Session with Kafka & HDFS support
# -------------------------------
spark = SparkSession.builder \
    .appName("OrionValley_StreamingAnomaly") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

# -------------------------------
# 2. Define schema for JSON messages
# -------------------------------
schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("rpm", IntegerType()),
    StructField("location", MapType(StringType, DoubleType()))
])

# -------------------------------
# 3. Read from Kafka
# -------------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot-sensor-data") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

# -------------------------------
# 4. Feature engineering (sliding window aggregates)
# -------------------------------
windowed_stats = df_raw \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        col("vehicle_id"),
        window(col("event_time"), "10 seconds", "5 seconds")
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("vibration").alias("avg_vib"),
        avg("rpm").alias("avg_rpm")
    )

# -------------------------------
# 5. Real‑time anomaly scoring (calling a model API)
#    For simplicity, we use a heuristic rule: if avg_vib > 3.0 -> anomaly
#    In production, you would call your KServe model endpoint.
# -------------------------------
def score_anomaly(row):
    # This function would be called on each row; for brevity, we use UDF
    return 1.0 if row["avg_vib"] > 3.0 else 0.0

from pyspark.sql.functions import udf
score_udf = udf(score_anomaly, DoubleType())

anomalies = windowed_stats.withColumn("anomaly_score", score_udf(col("avg_vib")))

# -------------------------------
# 6. Write to MongoDB (operational store)
# -------------------------------
def write_to_mongo(df, epoch_id):
    df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", "mongodb://localhost:27017/orion.anomalies") \
        .save()

# -------------------------------
# 7. Write raw data to HDFS (Parquet) for data lake
# -------------------------------
raw_stream = df_raw.selectExpr("vehicle_id", "timestamp", "temperature", "vibration", "rpm", "location")

hdfs_query = raw_stream.writeStream \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/raw_iot") \
    .option("checkpointLocation", "/tmp/hdfs_checkpoint") \
    .partitionBy("vehicle_id") \
    .trigger(processingTime="30 seconds") \
    .start()

# Write anomalies to MongoDB (use foreachBatch for custom logic)
mongo_query = anomalies.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

# Wait for both streams
hdfs_query.awaitTermination()
mongo_query.awaitTermination()
