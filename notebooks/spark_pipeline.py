from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, dayofweek, year, month, regexp_extract, expr, count, udf, collect_list, struct
from pyspark.sql.types import IntegerType, StringType
from math import radians, cos, sin, asin, sqrt
from sedona.register import SedonaRegistrator
import json

import os
# Ensure Spark finds the correct logging libraries
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.driver.extraJavaOptions=-Dlog4j2.formatMsgNoLookups=true pyspark-shell"

# Optionally specify an SLF4J implementation (logback)
os.environ["SPARK_CLASSPATH"] = "/home/ubuntu/team14/venv/lib/python3.12/site-packages/pyspark/jars/slf4j-log4j12-1.7.36.jar"

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

# Define JAR paths
JAR_PATHS = "/home/ubuntu/team14/jars/sedona-python-adapter-3.0_2.12-1.7.1.jar,/home/ubuntu/team14/jars/geotools-wrapper-1.5.0-29.2.jar"

spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.jars", JAR_PATHS) \
    .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions") \
    .getOrCreate()

# Initialize SedonaContext
sedona = SedonaContext.create(spark)

# Define file paths
crime_input_path = "data/processed/crime_final.csv"
ins_input_path = "data/processed/ins.csv"

crime_output_path = "data/processed/crime_final_spark.parquet"
ins_output_path = "data/processed/ins_final_spark.parquet"

# Load processed DuckDB data
crime = spark.read.csv(crime_input_path, header=True, inferSchema=True)
ins = spark.read.csv(ins_input_path, header=True, inferSchema=True)

###  CRIME DATA CLEANING ###
crime = crime.filter(crime["Premis Desc"].isNotNull() & crime["Crm Cd 1"].isNotNull())

# Convert 'Date Rptd' and 'DATE OCC' to timestamp
crime = crime.withColumn("DATE OCC",
    when(
        crime["DATE OCC"].rlike(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} [AP]M"),
        to_timestamp(crime["DATE OCC"], "MM/dd/yyyy hh:mm:ss a")
    ).otherwise(None))

# Extract day of the week and hour from 'DATE OCC'
crime = crime.withColumn("DayOfWeek OCC", dayofweek(col("DATE OCC")))
crime = crime.withColumn("TIME OCC", (col("TIME OCC") / 100).cast("int"))

# Map day of week to string labels
crime = crime.withColumn(
    "DayOfWeek OCC",
    when(col("DayOfWeek OCC") == 1, "Sunday")
    .when(col("DayOfWeek OCC") == 2, "Monday")
    .when(col("DayOfWeek OCC") == 3, "Tuesday")
    .when(col("DayOfWeek OCC") == 4, "Wednesday")
    .when(col("DayOfWeek OCC") == 5, "Thursday")
    .when(col("DayOfWeek OCC") == 6, "Friday")
    .when(col("DayOfWeek OCC") == 7, "Saturday")
)

# Fill null values in 'Vict Sex' and 'Vict Descent'
crime = crime.withColumn("Vict Sex", when(col("Vict Sex").isNull(), "Unidentified").otherwise(col("Vict Sex")))
crime = crime.withColumn("Vict Descent", when(col("Vict Descent").isNull(), "Unidentified").otherwise(col("Vict Descent")))


###  INSPECTION DATA CLEANING ###
ins = ins.filter(ins["Latitude/Longitude"].isNotNull())

# Extract latitude and longitude as float from "Latitude/Longitude"
ins = ins.withColumn("latitude", regexp_extract("Latitude/Longitude", r"\((-?\d+\.\d+),", 1).cast("float"))
ins = ins.withColumn("longitude", regexp_extract("Latitude/Longitude", r", (-?\d+\.\d+)\)", 1).cast("float"))
ins = ins.drop("Latitude/Longitude")

# Convert 'Inspection Date' into timestamp & extract Year/Month
ins = ins.withColumn("Inspection Date",
    when(ins["Inspection Date"].rlike(r"\d{2}/\d{2}/\d{4}"),
        to_timestamp(ins["Inspection Date"], "MM/dd/yyyy")
    ).otherwise(None))
ins = ins.withColumn("year", year(ins["Inspection Date"]))
ins = ins.withColumn("month", month(ins["Inspection Date"]))


###  SPATIAL JOIN: Find Inspections Near Crimes (200m Buffer) ###
ins = ins.withColumn("ins_point", expr("ST_Point(longitude, latitude)"))
crime = crime.withColumn("crime_point", expr("ST_Point(Lon, Lat)"))
crime = crime.withColumn("crime_buffer", expr("ST_AsText(ST_Buffer(crime_point, 200))"))
crime = crime.withColumn("crime_buffer", expr("ST_GeomFromWKT(crime_buffer)"))

# Perform spatial join: Find inspections inside crime buffers
joined_df = crime.alias("c").join(
    ins.alias("i"),
    expr("ST_Contains(c.crime_buffer, i.ins_point)"),  
    "left"
).groupby("c.Lon", "c.Lat").agg(count("i.ins_point").alias("inspection_count"))

###  GRID-BASED NEARBY INSPECTION COUNT (Haversine) ###
@udf(returnType=IntegerType())
def haversine_distance(lon1, lat1, lon2, lat2):
    """Calculate distance between two points in meters"""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon, dlat = lon2 - lon1, lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return int(6371000 * c)  # Earth's radius in meters

@udf(returnType=StringType())
def get_grid_id(lat, lon, precision=0.01):
    """Generate a grid ID for fast lookup"""
    return f"{int(lat/precision)},{int(lon/precision)}"

# Add grid IDs to both dataframes
crime = crime.withColumn("grid_id", get_grid_id(col("Lat"), col("Lon")))
ins = ins.withColumn("grid_id", get_grid_id(col("latitude"), col("longitude")))

# Aggregate inspection points by grid
ins_by_grid = ins.groupBy("grid_id").agg(
    collect_list(struct("longitude", "latitude")).alias("inspection_points")
)

# Convert to a Python dictionary for broadcasting
ins_grid_dict = {row["grid_id"]: row["inspection_points"] for row in ins_by_grid.collect()}

# Broadcast the dictionary
ins_grid_broadcast = spark.sparkContext.broadcast(ins_grid_dict)

@udf(returnType=IntegerType())
def count_nearby_inspections(grid_id, crime_lon, crime_lat):
    """Count inspections within 500m of a crime location"""
    inspection_points = ins_grid_broadcast.value.get(grid_id, [])
    count = 0
    for point in inspection_points:
        if haversine_distance(crime_lon, crime_lat, point["longitude"], point["latitude"]) <= 500:
            count += 1
    return count

# Apply the UDF to count nearby inspections for each crime
crime = crime.withColumn(
    "nearby_inspections_count",
    count_nearby_inspections(col("grid_id"), col("Lon"), col("Lat"))
)

###  SAVE OUTPUT ###
crime.write.parquet(crime_output_path)
ins.write.parquet(ins_output_path)

crime.show(5)
ins.show(5)

print("Processing completed successfully!")
