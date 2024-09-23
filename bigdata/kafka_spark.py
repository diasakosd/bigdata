from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg, unix_timestamp, from_json, expr
from pyspark.sql.types import TimestampType, StringType, DoubleType, IntegerType, StructType, StructField

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkProcessing") \
    .getOrCreate()

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vehicle_positions") \
    .load()

# Convert Kafka data to DataFrame
df = df.selectExpr("CAST(value AS STRING) AS json")

# Define schema for the incoming data, including simulator_start_time
schema = StructType([
    StructField("name", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("link", StringType(), True),
    StructField("simulator_start_time", StringType(), True),  # Include simulator_start_time
    StructField("position", DoubleType(), True),
    StructField("spacing", DoubleType(), True),
    StructField("speed", DoubleType(), True)
])

# Parse the JSON data into the schema
df = df.selectExpr("json").select(from_json(col("json"), schema).alias("data")).select("data.*")

# Convert 'time' and 'simulator_start_time' to timestamps
df = df.withColumn("time", unix_timestamp(col("time"), "dd/MM/yyyy HH:mm:ss").cast(TimestampType()))
df = df.withColumn("simulator_start_time", unix_timestamp(col("simulator_start_time"), "dd/MM/yyyy HH:mm:ss").cast(TimestampType()))

# Extract and process the required fields
result_df = df.groupBy(
    window(col("time"), "1 minute"),  # Window duration
    col("link")
).agg(
    count("name").alias("vcount"),       # Count of vehicles per link
    avg("speed").alias("vspeed"),        # Average speed of vehicles per link
    expr("min(simulator_start_time)").alias("min_simulator_start_time")  # Minimum simulator_start_time in each window
)

# Calculate the duration in seconds between simulator_start_time and the current window's start time
result_df = result_df.withColumn(
    "duration_since_start", 
    expr("unix_timestamp(current_timestamp()) - unix_timestamp(min_simulator_start_time)")  # Calculate difference in seconds
)

# Select only the necessary columns
result_df = result_df.select(
    col("window"), 
    col("link"), 
    col("vcount"), 
    col("vspeed"), 
    col("duration_since_start")
)

# Output the results to the console
query = result_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Await termination
query.awaitTermination()