# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import dayofweek, when, col, to_timestamp, regexp_extract, year, month
import duckdb

spark = SparkSession.builder \
    .appName("CSV ETL Pipeline") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
sc = spark.sparkContext

# TODO: change this to your directories
WORKING_DIR = "/Users/neifang/Downloads/final_405"
CRIME_FILE = os.path.join(WORKING_DIR, "crime.csv")
INS_FILE = os.path.join(WORKING_DIR, "ins.csv")
DB_FILE = os.path.join(WORKING_DIR, "my_database.db")
CRIME_OUTPUT = os.path.join(WORKING_DIR, "crime_final.csv")
INS_OUTPUT = os.path.join(WORKING_DIR, "ins_final.csv")

# Read CSV files
crime = spark.read.csv(CRIME_FILE, header=True, inferSchema=True)
ins = spark.read.csv(INS_FILE, header=True, inferSchema=True)

# Process crime data
crime = crime.filter(crime["Premis Desc"].isNotNull() & crime["Crm Cd 1"].isNotNull())

# Convert Date Rptd and Date OCC column from string to datetime
crime = crime.withColumn("Date Rptd", when(
    crime["Date Rptd"].rlike(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} [AP]M"),
    to_timestamp(crime["Date Rptd"], "MM/dd/yyyy hh:mm:ss a")
).otherwise(None))

crime = crime.withColumn("DATE OCC", when(
    crime["DATE OCC"].rlike(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} [AP]M"),
    to_timestamp(crime["DATE OCC"], "MM/dd/yyyy hh:mm:ss a")
).otherwise(None))

# Extract the day of the week info and hour info for OCC
crime = crime.withColumn("DayOfWeek OCC", dayofweek(crime["DATE OCC"]))
crime = crime.withColumn("TIME OCC", (col("TIME OCC") / 100).cast("int"))

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

# Fill null values
crime = crime.withColumn("Vict Sex", when(crime["Vict Sex"].isNull(), "Unidentified").otherwise(crime["Vict Sex"]))
crime = crime.withColumn("Vict Descent", when(crime["Vict Descent"].isNull(), "Unidentified").otherwise(crime["Vict Descent"]))

# Process inspection data
ins = ins.filter(ins["Latitude/Longitude"].isNotNull())
ins = ins.withColumn("latitude", regexp_extract("Latitude/Longitude", r"\((-?\d+\.\d+),", 1).cast("float"))
ins = ins.withColumn("longitude", regexp_extract("Latitude/Longitude", r", (-?\d+\.\d+)\)", 1).cast("float"))

# Drop the column called Latitude/Longitude from ins
ins = ins.drop("Latitude/Longitude")

ins = ins.withColumn("Inspection Date", when(
    ins["Inspection Date"].rlike(r"\d{2}/\d{2}/\d{4}"),
    to_timestamp(ins["Inspection Date"], "MM/dd/yyyy")
).otherwise(None))

# Extract the year and month info from inspection date
ins = ins.withColumn("year", year(ins["Inspection Date"]))
ins = ins.withColumn("month", month(ins["Inspection Date"]))
crime_temp_path = os.path.join(WORKING_DIR, "crime_temp.csv")
ins_temp_path = os.path.join(WORKING_DIR, "ins_temp.csv")

#crime.write.option("header", "true").csv(crime_temp_path, mode="overwrite")
#ins.write.option("header", "true").csv(ins_temp_path, mode="overwrite")

ins.coalesce(1).write.mode("overwrite").option("header", "true").csv(ins_temp_path)
crime.coalesce(1).write.mode("overwrite").option("header", "true").csv(crime_temp_path)


def get_csv_from_spark_output(directory):
    for file in os.listdir(directory):
        if file.startswith("part-") and file.endswith(".csv"):
            return os.path.join(directory, file)
    return None

crime_actual_path = get_csv_from_spark_output(crime_temp_path)
ins_actual_path = get_csv_from_spark_output(ins_temp_path)

print(f"PySpark processing completed. Data saved to intermediate files.")
spark.stop()
print("Starting DuckDB processing...")

# Now work with DuckDB
conn = duckdb.connect(DB_FILE)

# Import data into DuckDB
conn.execute(f"""
    CREATE TABLE IF NOT EXISTS inspection AS 
    SELECT * FROM read_csv_auto(
        '{ins_actual_path}', 
        HEADER=TRUE, 
        DELIM=','
    )
""")

conn.execute(f"""
    CREATE TABLE IF NOT EXISTS crime AS 
    SELECT * 
    FROM read_csv_auto(
        '{crime_actual_path}', 
        HEADER=TRUE, 
        DELIM=','
    )
""")

# Check if the column exists
existing_columns = conn.execute("DESCRIBE crime").fetchdf()['column_name'].tolist()

# Add column only if it doesn't exist
if 'downtown_distance' not in existing_columns:
    conn.execute("ALTER TABLE crime ADD COLUMN downtown_distance TEXT;")
    print("Column 'downtown_distance' added.")
else:
    print("Column 'downtown_distance' already exists.")

# Calculate distance from downtown LA
distance_query = """
SELECT 
    *,
    CASE
        WHEN distance_km <= 10 THEN '0-10km'
        WHEN distance_km <= 20 THEN '10-20km'
        ELSE '>20km'
    END AS distance_group
FROM (
    SELECT
        *,
        -- Fast distance approximation from Downtown LA using planar coordinates
        -- (111 km/degree for latitude, 92 km/degree for longitude at 34°N)
        SQRT(
            POW((LAT - 34.0522) * 111, 2) + 
            POW((LON - (-118.2437)) * 92, 2)
        ) AS distance_km
    FROM crime
) sub
"""

# Execute the distance query and update the table
conn.execute(f"CREATE OR REPLACE TABLE crime AS {distance_query}")

# Add column for nearby inspection count if it doesn't exist
conn.execute("ALTER TABLE crime ADD COLUMN IF NOT EXISTS nearby_inspection_count INTEGER DEFAULT 0")

# Update nearby inspection count
conn.execute("""
UPDATE crime 
SET nearby_inspection_count = (
    SELECT COUNT(*)
    FROM inspection
    WHERE 
        -- Bounding box filter (fast first pass)
        inspection.latitude BETWEEN crime.LAT - 0.005 AND crime.LAT + 0.005
        AND inspection.longitude BETWEEN crime.LON - 0.005 AND crime.LON + 0.005
        AND (POW((inspection.latitude - crime.LAT) * 111000, 2) + 
             POW((inspection.longitude - crime.LON) * 92000, 2)) < 250000  
);
""")

# Export final data to CSV
conn.execute(f"COPY crime TO '{CRIME_OUTPUT}' (FORMAT CSV, HEADER TRUE);")
conn.execute(f"COPY inspection TO '{INS_OUTPUT}' (FORMAT CSV, HEADER TRUE);")

conn.close()

print(f"DuckDB processing completed. Final data saved to:")
print(f"- Crime data: {CRIME_OUTPUT}")
print(f"- Inspection data: {INS_OUTPUT}")
