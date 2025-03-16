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

# Install GeoPandas separately (fixing Sedona import issue)
pip install geopandas

# Ensure Apache Sedona is properly installed
echo "Reinstalling Apache Sedona..."
pip uninstall -y apache-sedona
pip install apache-sedona

# Step 3: Download Sedona JAR dependencies
echo "Downloading necessary Sedona JAR dependencies..."

JARS_DIR="/home/ubuntu/team14/jars"
mkdir -p "$JARS_DIR"

# Define the URLs for the required JARs
SEDONA_JAR_URL="https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-3.5_2.12/1.7.0/sedona-spark-3.5_2.12-1.7.0.jar"
GEOTOOLS_JAR_URL="https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.5.0-29.2/geotools-wrapper-1.5.0-29.2.jar"

# Download the JARs only if they are missing
if [[ ! -f "$JARS_DIR/sedona-spark-3.5_2.12-1.7.0.jar" ]]; then
    wget -O "$JARS_DIR/sedona-spark-3.5_2.12-1.7.0.jar" "$SEDONA_JAR_URL"
fi

if [[ ! -f "$JARS_DIR/geotools-wrapper-1.5.0-29.2.jar" ]]; then
    wget -O "$JARS_DIR/geotools-wrapper-1.5.0-29.2.jar" "$GEOTOOLS_JAR_URL"
fi

echo "Sedona JAR dependencies downloaded."

# Step 4: Run PySpark processing first
echo "Running PySpark processing..."
if ! python3 notebooks/spark_pipeline.py; then
    echo "ERROR: Spark processing failed."
    exit 1
fi

# Step 5: Ensure Spark output files exist before running DuckDB
if [[ ! -f "$PROCESSED_DIR/ins_processed.parquet" ]]; then
    echo "ERROR: Spark processing failed. Expected files not found."
    exit 1
fi
echo "PySpark processing completed."

# Step 6: Now run DuckDB after Spark
echo "Running DuckDB data processing..."
if ! python3 notebooks/duckdb_pipeline.py; then
    echo "ERROR: DuckDB processing failed."
    exit 1
fi

# Step 7: Verify DuckDB output files exist
if [[ ! -f "$PROCESSED_DIR/crime_final.csv" || ! -f "$PROCESSED_DIR/ins.csv" ]]; then
    echo "ERROR: DuckDB processing failed. Output files not found."
    exit 1
fi
echo "DuckDB processing completed."

# Step 8: Transfer files back to local machine (if necessary)
echo "Transferring processed files to local machine..."
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:$PROCESSED_DIR/crime_final_spark.csv .
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:$PROCESSED_DIR/
