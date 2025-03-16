#!/bin/bash

set -e  # Exit immediately if any command fails

echo "Starting full pipeline execution..."

# Set the correct data directory
DATA_DIR="/home/ubuntu/team14/data"
PROCESSED_DIR="$DATA_DIR/processed"

# Ensure the processed directory exists
mkdir -p "$PROCESSED_DIR"

# Step 1: Verify that required data files exist
if [[ ! -f "$DATA_DIR/Crime_Data_from_2020_to_Present_20250304.csv" || ! -f "$DATA_DIR/Building_and_Safety_Inspections.csv" ]]; then
    echo "ERROR: Missing required data files in $DATA_DIR"
    exit 1
fi
echo "Data files found."

# Step 2: Install dependencies
echo "Installing necessary packages..."
pip install -r requirements.txt

# Step 3: Run PySpark processing first
echo "Running PySpark processing..."
python3 notebooks/spark_pipeline.py

# Step 4: Ensure Spark output files exist before running DuckDB
if [[ ! -f "$PROCESSED_DIR/ins_processed.parquet" ]]; then
    echo "ERROR: Spark processing failed. Expected files not found."
    exit 1
fi
echo "PySpark processing completed."

# Step 5: Now run DuckDB after Spark
echo "Running DuckDB data processing..."
python3 notebooks/duckdb_pipeline.py

# Step 6: Verify DuckDB output files exist
if [[ ! -f "$PROCESSED_DIR/crime_final.csv" || ! -f "$PROCESSED_DIR/ins.csv" ]]; then
    echo "ERROR: DuckDB processing failed. Output files not found."
    exit 1
fi
echo "DuckDB processing completed."

# Step 7: Transfer files back to local machine (if necessary)
echo "Transferring processed files to local machine..."
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:$PROCESSED_DIR/crime_final_spark.csv .
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:$PROCESSED_DIR/ins_final_spark.csv .
echo "Transfer complete. Check your local directory."

echo "Pipeline execution completed successfully."
