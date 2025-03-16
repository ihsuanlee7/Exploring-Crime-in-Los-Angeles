#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Crime and Inspection Data Processing") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Ensure necessary directories exist
os.makedirs("/home/ubuntu/data/processed", exist_ok=True)

# Define file paths on EC2
crime_input_path = "/home/ubuntu/data/processed/crime_final.csv"
ins_input_path = "/home/ubuntu/data/processed/ins.csv"
crime_output_path = "/home/ubuntu/data/processed/crime_final_spark.csv"
ins_output_path = "/home/ubuntu/data/processed/ins_final_spark.csv"

# Load processed DuckDB data
crime = spark.read.csv(crime_input_path, header=True, inferSchema=True)
ins = spark.read.csv(ins_input_path, header=True, inferSchema=True)

# Apply transformations (matching original logic)
crime = crime.withColumn("Year", year(col("DATE OCC"))).withColumn("Month", month(col("DATE OCC")))
ins = ins.withColumn("Year", year(col("Inspection Date"))).withColumn("Month", month(col("Inspection Date")))

# Save transformed data for Tableau (overwrite existing)
crime.write.csv(crime_output_path, header=True, mode="overwrite")
ins.write.csv(ins_output_path, header=True, mode="overwrite")

print("Spark processing completed. Transformed data saved for Tableau.")

# Stop Spark session
spark.stop()

