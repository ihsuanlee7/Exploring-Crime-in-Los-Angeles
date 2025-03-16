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
pip install --upgrade pip
pip install -r requirements.txt

# Ensure Apache Sedona is properly installed
pip uninstall -y apache-sedona
pip install apache-sedona

# Step 3: Download SLF4J No-Op Logger to prevent warnings
echo "Downloading necessary dependencies..."
wget -q -O slf4j-nop.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-nop/1.7.30/slf4j-nop-1.7.30.jar
echo "SLF4J logging JAR downloaded."

# Step 4: Set SPARK_CLASSPATH for PySpark to use SLF4J
export SPARK_CLASSPATH=$PWD/slf4j-nop.jar

# Step 5: Run PySpark processing first
echo "Running PySpark processing..."
python3 notebooks/spark_pipeline.py

# Step 6: Ensure Spark output files exist before running DuckDB
if [[ ! -f "$PROCESSED_DIR/ins_processed.parquet" ]]; then
    echo "ERROR: Spark processing failed. Expected files not found."
    exit 1
fi
echo "PySpark processing completed."

# Step 7: Now run DuckDB after Spark
echo "Running DuckDB data processing..."
python3 notebooks/duckdb_pipeline.py

# Step 8: Verify DuckDB output files exist
if [[ ! -f "$PROCESSED_DIR/crime_final.csv" || ! -f "$PROCESSED_DIR/ins.csv" ]]; then
    echo "ERROR: DuckDB processing failed. Output files not found."
    exit 1
fi
echo "DuckDB processing completed."

# Step 9: Transfer files back to local machine (if necessary)
echo "Transferring processed files to local machine..."
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:$PROCESSED_DIR/crime_final_spark.csv .
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:$PROCESSED_DIR/ins_final_spark.csv .
echo "Transfer complete. Check your local directory."

echo "Pipeline execution completed successfully."
