import duckdb
import os

# Define base directories
DATA_DIR = "/home/ubuntu/team14/data"
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")

# Ensure processed directory exists
os.makedirs(PROCESSED_DIR, exist_ok=True)

# File paths
crime_input_path = os.path.join(DATA_DIR, "Crime_Data_from_2020_to_Present_20250304.csv")
inspection_input_path = os.path.join(DATA_DIR, "Building_and_Safety_Inspections.csv")
db_path = os.path.join(PROCESSED_DIR, "my_database.db")
csv_output_path = os.path.join(PROCESSED_DIR, "crime_final.csv")

# Connect to DuckDB
conn = duckdb.connect(db_path)

# Step 1: Create `inspection` table FIRST
conn.execute(f"""
    CREATE TABLE IF NOT EXISTS inspection AS 
    SELECT * 
    FROM read_csv_auto(
        '{inspection_input_path}', 
        HEADER=TRUE, 
        DELIM=','
    )
""")

# Step 2: Create `crime` table
conn.execute(f"""
    CREATE TABLE IF NOT EXISTS crime AS 
    SELECT * 
    FROM read_csv_auto(
        '{crime_input_path}', 
        HEADER=TRUE, 
        DELIM=','
    )
""")

# Ensure `crime` table has `downtown_distance` column
conn.execute("ALTER TABLE crime ADD COLUMN IF NOT EXISTS downtown_distance TEXT;")

# Compute downtown distance categories
conn.execute("""
UPDATE crime
SET downtown_distance = 
    CASE
        WHEN SQRT(
                POW((LAT - 34.0522) * 111, 2) + 
                POW((LON - (-118.2437)) * 92, 2)
            ) <= 10 THEN '0-10km'
        WHEN SQRT(
                POW((LAT - 34.0522) * 111, 2) + 
                POW((LON - (-118.2437)) * 92, 2)
            ) <= 20 THEN '10-20km'
        ELSE '>20km'
    END;
""")

# Ensure `crime` table has `nearby_inspection_count` column
conn.execute("ALTER TABLE crime ADD COLUMN IF NOT EXISTS nearby_inspection_count INTEGER DEFAULT 0;")

# Update `crime` with nearby inspection counts
conn.execute("""
UPDATE crime 
SET nearby_inspection_count = (
    SELECT COUNT(*)
    FROM inspection
    WHERE 
        inspection.latitude BETWEEN crime.LAT - 0.005 AND crime.LAT + 0.005
        AND inspection.longitude BETWEEN crime.LON - 0.005 AND crime.LON + 0.005
        AND (POW((inspection.latitude - crime.LAT) * 111000, 2) + 
             POW((inspection.longitude - crime.LON) * 92000, 2)) < 250000  
);
""")

# Export the processed crime data to CSV
conn.execute(f"COPY crime TO '{csv_output_path}' (FORMAT CSV, HEADER TRUE);")

# Close connection
conn.close()
