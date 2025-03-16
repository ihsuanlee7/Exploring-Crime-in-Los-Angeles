#!/bin/bash

set -e  # Exit immediately if any command fails

echo "Starting full pipeline execution..."

# Set data directory paths
DATA_DIR="/home/ubuntu/team14/data"
PROCESSED_DIR="$DATA_DIR/processed"

# Ensure processed directory exists
mkdir -p "$PROCESSED_DIR"

# Step 1: Verify required data files exist
if [[ ! -f "$DATA_DIR/Crime_Data_from_2020_to_Present_20250304.csv" || ! -f "$DATA_DIR/Building_and_Safety_Inspections.csv" ]]; then
    echo "ERROR: Missing required data files in $DATA_DIR"
    exit 1
fi
echo "Data files found."

# Step 2: Install dependencies
echo "Installing necessary Python packages..."
pip install --upgrade pip
pip install -r requirements.txt

# Ensure Apache Sedona is properly installed
echo "Reinstalling Apache Sedona..."
pip uninstall -y apache-sedona
pip install apache-sedona==1.6.1

# Step 3: Download SLF4J, Log4J, and Sedona dependencies
echo "Downloading necessary JAR dependencies..."
mkdir -p jars

# Download SLF4J No-Op Logger (Suppress warnings)
wget -q -O jars/slf4j-nop.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-nop/1.7.30/slf4j-nop-1.7.30.jar

# Download Log4J dependencies
wget -q -O jars/log4j-core.jar https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/2.17.1/log4j-core-2.17.1.jar
wget -q -O jars/log4j-api.jar https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.17.1/log4j-api-2.17.1.jar

# Download Sedona dependencies
if [[ ! -f "jars/sedona-python-adapter-3.0_2.12-1.7.1.jar" ]]; then
    echo "Downloading Sedona Python Adapter JAR..."
    wget -q -O jars/sedona-python-adapter-3.0_2.12-1.7.1.jar \
        https://repo1.maven.org/maven2/org/apache/sedona/sedona-python-adapter-3.0_2.12/1.7.1/sedona-python-adapter-3.0_2.12-1.7.1.jar
fi

if [[ ! -f "jars/geotools-wrapper-1.5.0-29.2.jar" ]]; then
    echo "Downloading GeoTools Wrapper JAR..."
    wget -q -O jars/geotools-wrapper-1.5.0-29.2.jar \
        https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.5.0-29.2/geotools-wrapper-1.5.0-29.2.jar
fi

echo "JAR dependencies downloaded successfully."

# Proceed to PySpark Processing
export SPARK_CLASSPATH="/home/ubuntu/team14/jars/sedona-python-adapter-3.0_2.12-1.7.1.jar:/home/ubuntu/team14/jars/geotools-wrapper-1.5.0-29.2.jar"

echo "Running PySpark processing..."
if ! python3 notebooks/spark_pipeline.py; then
    echo "ERROR: Spark processing failed."
    exit 1
fi
echo "PySpark processing completed."

# Verify Spark output files before running DuckDB
if [[ ! -f "$PROCESSED_DIR/ins_processed.parquet" ]]; then
    echo "ERROR: Spark processing did not produce expected output."
    exit 1
fi

# Step 4: Run DuckDB processing
echo "Running DuckDB data processing..."
if ! python3 notebooks/duckdb_pipeline.py; then
    echo "ERROR: DuckDB processing failed."
    exit 1
fi
echo "DuckDB processing completed."

# Step 5: Verify DuckDB output files exist
if [[ ! -f "$PROCESSED_DIR/crime_final.csv" || ! -f "$PROCESSED_DIR/ins.csv" ]]; then
    echo "ERROR: DuckDB processing failed. Output files not found."
    exit 1
fi

# Step 6: Transfer files back to local machine
echo "Transferring processed files to local machine..."
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:$PROCESSED_DIR/crime_final_spark.csv .
scp -i MGMTMSA405.pem ubuntu@54.185.234.165:$PROCESSED_DIR/ins_final_spark.csv .
echo "Transfer complete. Check your local directory."

echo "Pipeline execution completed successfully."
