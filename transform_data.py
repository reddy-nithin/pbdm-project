# transform_data.py
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, count

# --- Configuration ---
DB_NAME = "kc_311_db"
RAW_COLLECTION = "raw_requests"
MART_COLLECTION = "mart_daily_summary"

# MongoDB Spark Connector package. Spark will download this automatically.
MONGO_SPARK_CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1"

def transform_data(mongo_connection_string):
    """
    Reads raw 311 data from MongoDB, transforms and aggregates it using Spark,
    and writes the result to a mart collection.
    :param mongo_connection_string: The MongoDB connection string.
    """
    if not mongo_connection_string:
        print("ERROR: MongoDB connection string was not provided.", file=sys.stderr)
        sys.exit(1)

    # The Spark connector requires the database and collection in the URI.
    # We construct these URIs programmatically for clarity and correctness.
    read_uri = f"{mongo_connection_string}/{DB_NAME}.{RAW_COLLECTION}"
    write_uri = f"{mongo_connection_string}/{DB_NAME}.{MART_COLLECTION}"

    spark = None
    try:
        print("Initializing Spark session...")
        # Initialize a SparkSession with the MongoDB connector configuration.
        # This is the most reliable method to ensure Spark uses the correct URIs.
        spark = SparkSession.builder \
            .appName("KC311MongoTransformation") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.jars.packages", MONGO_SPARK_CONNECTOR) \
            .config("spark.mongodb.read.connection.uri", read_uri) \
            .config("spark.mongodb.write.connection.uri", write_uri) \
            .getOrCreate()

        print("Spark session initialized.")

        # --- 1. READ raw data from MongoDB into a Spark DataFrame ---
        print(f"Reading raw data from '{RAW_COLLECTION}' collection...")
        # The connector will automatically use the 'spark.mongodb.read.connection.uri'
        # configuration we set in the SparkSession.
        df = spark.read.format("mongodb").load()

        # --- 2. TRANSFORM the data ---
        print("Transforming data...")
        mart_df = df.select(
            col("open_date_time").cast("timestamp").alias("creation_date"),
            col("issue_type").alias("category"),
            col("current_status").alias("status")
        ).filter(col("issue_type").isNotNull()) \
         .withColumn("request_date", date_trunc("day", col("creation_date"))) \
         .groupBy("request_date", "category", "status") \
         .agg(count("*").alias("number_of_requests"))

        print("Transformation complete. Schema of the final DataFrame:")
        mart_df.printSchema()

        # --- 3. WRITE the transformed data back to MongoDB ---
        print(f"Writing transformed data to '{MART_COLLECTION}' collection...")
        # The connector will automatically use the 'spark.mongodb.write.connection.uri'
        # configuration we set in the SparkSession.
        mart_df.write.format("mongodb").mode("overwrite").save()
        print("Write operation complete.")

    except Exception as e:
        print(f"An error occurred during the Spark job: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if spark:
            print("Stopping Spark session.")
            spark.stop()

if __name__ == "__main__":
    # The connection string is passed as the first command-line argument
    if len(sys.argv) != 2:
        print("Usage: spark-submit transform_data.py \"<mongo_connection_string>\"", file=sys.stderr)
        sys.exit(-1)
    
    transform_data(sys.argv[1])
    print("\nTransformation script finished successfully.")