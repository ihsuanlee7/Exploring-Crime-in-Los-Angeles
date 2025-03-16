#!/bin/bash

set -e  # Exit immediately if any command fails

echo "Starting full pipeline execution..."

# Set the correct data directory
DATA_DIR="/home/ubuntu/team14/data"
PROCESSED_DIR="$DATA_DIR/processed"

# Ensure the processed directory exists
mkdir -p "$PROCESSED_DIR"

# Step 1: Verify required data files
if [[ ! -f "$DATA_DIR/Crime_Data_from_2020_to_Present_20250304.csv" || ! -f "$DATA_DIR/Building_and_Safety_Inspections.csv" ]]; then
    echo "ERROR: Missing required data files in $DATA_DIR"
    exit 1
fi
echo "Data files found."

# Step 2: Install dependencies
echo "Installing necessary packages..."
pip install --upgrade pip
pip install -r requirements.txt

# Step 3: Ensure Apache Sedona is installed
echo "Reinstalling Apache Sedona..."
pip uninstall -y apache-sedona
pip install apache-sedona==1.7.1  # Ensure version consistency

# Step 4: Download necessary Sedona JAR dependencies
echo "Downloading necessary Sedona JAR dependencies..."
mkdir -p /home/ubuntu/team14/jars

JAR_PATH="/home/ubuntu/team14/jars/sedona-spark-3.5_2.12-1.7.1.jar"
if [[ ! -f "$JAR_PATH" ]]; then
    wget -O "$JAR_PATH" "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-3.5_2.12/1.7.1/sedona-spark-3.5_2.12-1.7.1.jar"
fi
echo "Sedona JAR dependencies downloaded."

# Step 5: Run PySpark processing
echo "Running PySpark processing..."
if ! python3 notebooks/spark_pipeline.py; then
    echo "ERROR: Spark processing failed."
    exit 1
fi

# Step 6: Run DuckDB processing
echo "Running DuckDB data processing..."
if ! python3 notebooks/duckdb_pipeline.py; then
    echo "ERROR: DuckDB processing failed."
    exit 1
fi

echo "Pipeline execution completed successfully."
