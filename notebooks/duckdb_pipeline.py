#!/usr/bin/env python
# coding: utf-8

# In[1]:


import duckdb
import os
import pandas as pd

# Ensure necessary directories exist
os.makedirs("/home/ubuntu/data/processed", exist_ok=True)

# Define file paths
crime_input_path = "/home/ubuntu/data/Crime_Data_from_2020_to_Present_20250304.csv"
ins_input_path = "/home/ubuntu/data/Building_and_Safety_Inspections.csv"
crime_output_path = "/home/ubuntu/data/processed/crime_final.csv"
ins_output_path = "/home/ubuntu/data/processed/ins.csv"

# Connect to DuckDB in-memory database
con = duckdb.connect(database=":memory:")

# Load raw crime data
crime_df = pd.read_csv(crime_input_path)
con.register("crime_raw", crime_df)

# Select relevant columns & transform
crime_final = con.execute("""
    SELECT 
        "DATE OCC",
        "AREA NAME",
        "Crm Cd Desc",
        "Vict Age",
        "Vict Sex",
        "Vict Descent"
    FROM crime_raw
""").df()

# Save processed crime data
crime_final.to_csv(crime_output_path, index=False)

# Load raw inspection data
ins_df = pd.read_csv(ins_input_path)
con.register("ins_raw", ins_df)

# Select relevant columns & transform
ins_final = con.execute("""
    SELECT 
        "Inspection Date",
        "Latitude/Longitude",
        "Action Taken",
        "Result",
        "Inspection Type"
    FROM ins_raw
""").df()

# Save processed inspection data
ins_final.to_csv(ins_output_path, index=False)

print("DuckDB processing completed. Processed data saved.")

# Close connection
con.close()

