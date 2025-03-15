#!/bin/bash

echo "Starting the data pipeline..."

# Step 1: Download Data
echo "Downloading data..."
python scripts/download_data.py

# Step 2: Run the DuckDB notebook (Preprocessing)
echo "Running DuckDB pipeline..."
papermill notebooks/duckdb_pipeline.ipynb notebooks/output_duck.ipynb

# Step 3: Run the Spark notebook (Big Data Processing)
echo "Running Spark pipeline..."
papermill notebooks/spark_pipeline.ipynb notebooks/output_spark.ipynb

# Step 4: Convert output to Tableau format
echo "Converting data to Tableau format..."
python scripts/tableau_update.py

# Step 5: Refresh Tableau Dashboard (if needed)
echo "Refreshing Tableau Dashboard..."
tabcmd refreshextracts --workbook "YourWorkbook"

echo " Pipeline execution completed!"
