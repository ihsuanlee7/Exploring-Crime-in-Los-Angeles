import duckdb
import os

# Define base directory relative to script location
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
crime_input_path = os.path.join(base_dir, "Crime_Data_from_2020_to_Present_20250304.csv")
inspection_input_path = os.path.join(base_dir, "Building_and_Safety_Inspections.csv")
db_path = os.path.join(base_dir, "processed", "my_database.db")
csv_output_path = os.path.join(base_dir, "processed", "crime_final.csv")

# Ensure processed directory exists
os.makedirs(os.path.dirname(db_path), exist_ok=True)

# Connect to DuckDB database
conn = duckdb.connect(db_path)

# Create and load the inspection table
conn.execute(f"""
    CREATE TABLE IF NOT EXISTS inspection AS 
    SELECT * 
    FROM read_csv_auto(
        '{inspection_input_path}', 
        HEADER=TRUE, 
        DELIM=','
    )
""")

# Create and load the crime table
conn.execute(f"""
    CREATE TABLE IF NOT EXISTS crime AS 
    SELECT * 
    FROM read_csv_auto(
        '{crime_input_path}', 
        HEADER=TRUE, 
        DELIM=','
    )
""")

# Add downtown_distance column if it doesn't exist
conn.execute("ALTER TABLE crime ADD COLUMN IF NOT EXISTS downtown_distance TEXT;")

# Compute downtown distance categories using the correct logic
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

# Alternative distance query (for verification, but not needed to run separately)
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
        SQRT(
            POW((LAT - 34.0522) * 111, 2) + 
            POW((LON - (-118.2437)) * 92, 2)
        ) AS distance_km
    FROM crime
) sub
"""

# Add nearby_inspection_count column if it doesn't exist
conn.execute("ALTER TABLE crime ADD COLUMN IF NOT EXISTS nearby_inspection_count INTEGER DEFAULT 0;")

# Update crime table with nearby inspection counts
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
