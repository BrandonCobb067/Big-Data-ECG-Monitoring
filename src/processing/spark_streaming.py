"""
Central big-data computer layer for PySpark Structured Streaming.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType
import pandas as pd
import numpy as np

import src.config as config
from src.database.mongo_writer import spark_mongo_sink

# Define the schema matching the JSON payload sent from stream_simulator.py
payload_schema = StructType([
    StructField("node_id", IntegerType(), True),
    StructField("pipeline_mode", StringType(), True),
    StructField("original_fs", IntegerType(), True),
    StructField("transmitted_fs", IntegerType(), True),
    StructField("data", ArrayType(ArrayType(FloatType())), True)
])

@pandas_udf(ArrayType(ArrayType(FloatType())))
def z_score_normalize_udf(data_series: pd.Series) -> pd.Series:
    """
    Vectorized Pandas UDF to execute Z-score normalization across Spark worker nodes.
    """
    def normalize_array(matrix):
        if matrix is None:
            return None
            
        # Convert PySpark's 1D object array of nested lists into a proper 2D numeric array
        arr = np.array(matrix.tolist(), dtype=float)
        
        # Calculate mean and standard deviation along the time axis
        mean = np.mean(arr, axis=0)
        std = np.std(arr, axis=0)
        
        # Prevent division by zero
        std = np.where(std == 0, 1e-6, std)
        normalized = (arr - mean) / std
        return normalized.tolist()

    return data_series.apply(normalize_array)

def start_streaming_job():
    """
    Initializes the PySpark Structured Streaming context and processes incoming TCP traffic.
    """
    spark = SparkSession.builder \
            .appName("ECG_Distributed_Processor") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config ("spark.executor.memory", "4g") \
            .getOrCreate()

    # Suppress excessive logging
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read from Kafka and apply memory throttle
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option ("subscribe", "ecg_topic") \
        .option("maxOffsetsPerTrigger", 5000) \
        .load()

    # 2. Kafka values arrive as binary. Cast to string before parsing JSON.
    parsed_stream = raw_stream.selectExpr("Cast(value AS STRING)") \
        .select(from_json(col("value"), payload_schema).alias("parsed")) \
        .select("parsed.*")

    # 3. Execute parallelized statistical transformations
    processed_stream = parsed_stream.withColumn(
        "normalized_data",
        z_score_normalize_udf(col("data"))
    )

    # 4. Route processed data to the MongoDB storage layer
    query = processed_stream.writeStream \
        .foreachBatch(spark_mongo_sink) \
        .outputMode("append") \
        .option("checkpointLocation", "./spark_checkpoints") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_streaming_job()