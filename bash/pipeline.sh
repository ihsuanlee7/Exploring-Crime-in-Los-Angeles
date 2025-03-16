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

# Step 3: Download Sedona JAR if it doesn't exist
SEDONA_JAR_PATH="/home/ubuntu/team14/libs/sedona-spark-shaded-1.7.1-jar-with-dependencies.jar"
if [ ! -f "$SEDONA_JAR_PATH" ]; then
    echo "Downloading Sedona JAR file..."
    wget https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded/1.7.1/sedona-spark-shaded-1.7.1-jar-with-dependencies.jar -P /home/ubuntu/team14/libs/
    echo "Sedona JAR downloaded successfully."
else
    echo "Sedona JAR already exists at $SEDONA_JAR_PATH"
fi

# Step 4: Set classpath for Spark
export SPARK_CLASSPATH=$SEDONA_JAR_PATH

# Step 5: Download SLF4J and Log4J dependencies
echo "Downloading SLF4J and Log4J dependencies..."
mkdir -p jars

# Download SLF4J No-Op Logger (Suppress warnings)
wget -q -O jars/slf4j-nop.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-nop/1.7.30/slf4j-nop-1.7.30.jar

# Download Log4J dependencies
wget -q -O jars/log4j-core.jar https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/2.17.1/log4j-core-2.17.1.jar
wget -q -O jars/log4j-api.jar https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.17.1/log4j-api-2.17.1.jar

echo "Log4J and SLF4J dependencies downloaded."

# Set classpath for Spark (adding SLF4J and Log4J dependencies)
export SPARK_CLASSPATH=$SPARK_CLASSPATH:$PWD/jars/slf4j-nop.jar:$PWD/jars/log4j-core.jar:$PWD/jars/log4j-api.jar

# Step 6: Run PySpark processing first
echo "Running PySpark processing..."
if ! python3 notebooks/spark_pipeline.py; then
    echo "ERROR: Spark processing failed."
    exit 1
fi

# Step 7: Ensure Spark output files exist before running DuckDB
if [[ ! -f "$PROCESSED_DIR/ins_processed.parquet" ]]; then
    echo "ERROR: Spark processing failed. Expected files not found."
    exit 1
fi
echo "PySpark processing completed."

# Step 8: Now run DuckDB after Spark
echo "Running DuckDB data processing..."
if ! python3 notebooks/duckdb_pipeline.py; then
    echo "ERROR: DuckDB processing failed."
    exit 1
fi

# Step 9: Verify DuckDB output files exist
if [[ ! -f "$PROCESSED_DIR/crime_final.csv" || ! -f "$PROCESSED_DIR/ins.csv" ]]; then
    echo "ERROR: DuckDB processing failed. Output files not found."
    exit 1
fi
echo "DuckDB processing completed."

# Step 10: Transfer files back to local machine (if necessary)
echo "Transferring processed files to local machine..."
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:$PROCESSED_DIR/crime_final_spark.csv .
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:$PROCESSED_DIR/ins_final_spark.csv .
echo "Transfer complete. Check your local directory."

echo "Pipeline execution completed successfully."
