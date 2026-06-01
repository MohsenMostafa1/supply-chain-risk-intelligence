from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, MapType
import requests
import logging

# -------------------------------
# Helper function (no Spark dependency)
# -------------------------------
def score_anomaly(row):
    """Return 1.0 if anomaly (vibration > 3.0), else 0.0."""
    return 1.0 if row.get("avg_vib", 0) > 3.0 else 0.0

# -------------------------------
# Main execution – only runs when script is executed directly
# -------------------------------
if __name__ == "__main__":
    # 1. Spark Session
    spark = SparkSession.builder \
        .appName("OrionValley_StreamingAnomaly") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .getOrCreate()

    # 2. Schema definition (fixed MapType)
    schema = StructType([
        StructField("vehicle_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("temperature", DoubleType()),
        StructField("vibration", DoubleType()),
        StructField("rpm", IntegerType()),
        StructField("location", MapType(StringType(), DoubleType()))
    ])

    # 3. Read from Kafka
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

    # 4. Windowed aggregates
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

    # 5. Apply anomaly scoring (using the helper function)
    from pyspark.sql.functions import udf
    score_udf = udf(score_anomaly, DoubleType())
    anomalies = windowed_stats.withColumn("anomaly_score", score_udf(col("avg_vib")))

    # 6. Write anomalies to MongoDB (using foreachBatch)
    def write_to_mongo(df, epoch_id):
        df.write \
            .format("mongo") \
            .mode("append") \
            .option("uri", "mongodb://localhost:27017/orion.anomalies") \
            .save()

    # 7. Write raw data to HDFS (Parquet)
    raw_stream = df_raw.selectExpr("vehicle_id", "timestamp", "temperature", "vibration", "rpm", "location")

    hdfs_query = raw_stream.writeStream \
        .format("parquet") \
        .option("path", "hdfs://localhost:9000/user/raw_iot") \
        .option("checkpointLocation", "/tmp/hdfs_checkpoint") \
        .partitionBy("vehicle_id") \
        .trigger(processingTime="30 seconds") \
        .start()

    mongo_query = anomalies.writeStream \
        .foreachBatch(write_to_mongo) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    # 8. Wait for termination
    hdfs_query.awaitTermination()
    mongo_query.awaitTermination()
