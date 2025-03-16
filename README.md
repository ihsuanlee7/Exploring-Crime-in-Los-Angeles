# Repository for Team 14

## Datasets
For this analysis we used the following datasets:

Listing of All Businesses (260MB): [link](https://data.lacity.org/Administration-Finance/Listing-of-All-Businesses/r4uk-afju/about_data)<br>
A dataset containing businesses located in Los Angeles, including business name, address, basic business description, start & end date, etc. (Updated Feb 2025)

Crime Data from 2020 to Present (248MB): [link](https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8/about_data)<br>
A dataset containing records of reported crimes in Los Angeles, including date, location(Latitude and Longitude), crime type, crime detail, etc. (Updated Feb 2025)

## Pipeline logic
The pipeline automates downloading the two large dataset and running the PySpark and DuckDB scripts. This process outputs `.csv` files that are used to populate the Tableau dashboard.

## Pipeline automation
1. Connect to your EC2 instance.
2. Clone this repository (only if you have not already): `git clone https://github.com/Prof-Rosario-UCLA/team14.git`
3. Enter the team directory: `cd team14`
4. Activate the virtual environment: `source venv/bin/activate`
5. Run the pipeline: `bash bash/pipeline.sh`
