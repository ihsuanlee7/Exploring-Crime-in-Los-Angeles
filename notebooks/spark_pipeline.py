from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

# Define the JAR path
sedona_jar_path = "/home/ubuntu/team14/jars/sedona-spark-3.5_2.12-1.7.1.jar"

# Initialize SparkSession with the correct Sedona JAR
spark = SparkSession.builder \
    .appName("Sedona Test") \
    .config("spark.jars", sedona_jar_path) \
    .getOrCreate()

# Ensure Sedona is initialized correctly
try:
    sedona = SedonaContext.create(spark)
    print("SedonaContext initialized successfully.")
except Exception as e:
    print(f"ERROR initializing SedonaContext: {e}")
    exit(1)
