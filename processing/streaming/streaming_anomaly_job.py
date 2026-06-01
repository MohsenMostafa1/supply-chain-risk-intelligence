from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, MapType

# -------------------------------
# Pure Python helper function – no Spark dependencies
# This can be imported by unit tests safely.
# -------------------------------
def score_anomaly(row):
    """Return 1.0 if anomaly (vibration > 3.0), else 0.0."""
    return 1.0 if row.get("avg_vib", 0) > 3.0 else 0.0


# -------------------------------
# Main entry point – all Spark code goes here
# -------------------------------
if __name__ == "__main__":
    # Spark session
    spark = SparkSession.builder \
        .appName("OrionValley_StreamingAnomaly") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .getOrCreate()

    # Schema definition (fixed: use instances, not classes)
    schema = StructType([
        StructField("vehicle_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("temperature", DoubleType()),
        StructField("vibration", DoubleType()),
        StructField("rpm", IntegerType()),
        StructField("location", MapType(StringType(), DoubleType()))   # Note parentheses
    ])

    # Read from Kafka
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

    # Windowed aggregates
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

    # Anomaly scoring UDF
    score_udf = udf(score_anomaly, DoubleType())
    anomalies = windowed_stats.withColumn("anomaly_score", score_udf(col("avg_vib")))

    # Write to MongoDB
    def write_to_mongo(df, epoch_id):
        df.write \
            .format("mongo") \
            .mode("append") \
            .option("uri", "mongodb://localhost:27017/orion.anomalies") \
            .save()

    # Write raw data to HDFS
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

    hdfs_query.awaitTermination()
    mongo_query.awaitTermination()
