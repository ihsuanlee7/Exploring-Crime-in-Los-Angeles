#!/bin/bash

echo "Starting the ETL pipeline..."

# Step 1: Ensure large dataset files exist
if [ ! -f "data/raw/Crime_Data_from_2020_to_2024.csv" ] || [ ! -f "data/raw/Building_and_Safety_Inspections.csv" ]; then
    echo "Error: Required datasets are missing in data/raw/. Please upload them to EC2 before running."
    exit 1
fi

# Step 2: Run DuckDB pipeline
echo "Running DuckDB pipeline..."
papermill notebooks/duckdb_pipeline.ipynb notebooks/output_duck.ipynb
if [ $? -ne 0 ]; then
    echo "DuckDB pipeline failed. Exiting."
    exit 1
fi

# Step 3: Run Spark pipeline
echo "Running Spark pipeline..."
papermill notebooks/spark_pipeline.ipynb notebooks/output_spark.ipynb
if [ $? -ne 0 ]; then
    echo "Spark pipeline failed. Exiting."
    exit 1
fi

# Step 4: Confirm processed files exist
if [ -f "data/processed/crime_final_spark.csv" ] && [ -f "data/processed/ins_final_spark.csv" ]; then
    echo "Processed data saved successfully in data/processed/. Ready for Tableau."
else
    echo "Error: Output files not found. Something went wrong."
    exit 1
fi

exit 0
