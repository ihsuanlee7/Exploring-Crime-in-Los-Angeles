#!/bin/bash

echo "Starting the ETL pipeline..."

# Step 1: Run DuckDB pipeline
echo "Running DuckDB pipeline..."
papermill notebooks/duckdb_pipeline.ipynb notebooks/output_duck.ipynb
if [ $? -ne 0 ]; then
    echo "DuckDB pipeline failed. Exiting."
    exit 1
fi

# Step 2: Run Spark pipeline
echo "Running Spark pipeline..."
papermill notebooks/spark_pipeline.ipynb notebooks/output_spark.ipynb
if [ $? -ne 0 ]; then
    echo "Spark pipeline failed. Exiting."
    exit 1
fi

# Step 3: Confirm output files exist
if [ -f "data/processed/crime_final_spark.csv" ] && [ -f "data/processed/ins_final_spark.csv" ]; then
    echo "Processed data saved successfully. Ready for Tableau."
else
    echo "Output files not found. Something went wrong."
    exit 1
fi

exit 0
