#!/bin/bash

set -e  # Exit script on error

echo "Starting full pipeline execution..."

# Step 1: Verify data files exist
if [[ ! -f "/home/ubuntu/data/Crime_Data_from_2020_to_Present_20250304.csv" || ! -f "/home/ubuntu/data/Building_and_Safety_Inspections.csv" ]]; then
    echo "ERROR: Missing required data files. Please check /home/ubuntu/data/"
    exit 1
fi
echo "Data files found."

# Step 2: Install dependencies
echo "Installing necessary packages..."
pip install -r requirements.txt

# Step 3: Run DuckDB script
echo "Running DuckDB data processing..."
python3 duckdb_pipeline.py

# Step 4: Check DuckDB output files
if [[ ! -f "/home/ubuntu/data/processed/crime_final.csv" || ! -f "/home/ubuntu/data/processed/ins.csv" ]]; then
    echo "ERROR: DuckDB output files not found."
    exit 1
fi
echo "DuckDB processing completed."

# Step 5: Run PySpark script
echo "Running PySpark processing..."
python3 spark_pipeline.py

# Step 6: Check PySpark output files
if [[ ! -f "/home/ubuntu/data/processed/crime_final_spark.csv" || ! -f "/home/ubuntu/data/processed/ins_final_spark.csv" ]]; then
    echo "ERROR: Spark output files not found."
    exit 1
fi
echo "PySpark processing completed."

# Step 7: Transfer files back to local machine (optional)
echo "Transferring files to local machine..."
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:/home/ubuntu/data/processed/crime_final_spark.csv .
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:/home/ubuntu/data/processed/ins_final_spark.csv .
echo "Transfer complete. Check your local directory."

echo "Pipeline execution completed successfully."
