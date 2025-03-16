# Repository for Team 14

## Datasets
For this analysis we used the following datasets:

Listing of All Businesses(260MB): [link](https://data.lacity.org/Administration-Finance/Listing-of-All-Businesses/r4uk-afju/about_data)<br>
A dataset containing businesses located in Los Angeles, including business name, address, basic business description, start & end date, etc. (Updated Feb 2025)

Crime Data from 2020 to Present (248MB): [link](https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8/about_data)<br>
A dataset containing records of reported crimes in Los Angeles, including date, location(Latitude and Longitude), crime type, crime detail, etc. (Updated Feb 2025)

After importing the files, the pipeline first runs the Spark script and then runs the DuckDB script. This outputs csv files that are used to populate the Tableau dashboard.

## Pipeline automation
1. Enter an EC2 instance.
2. Enter a virtual machine:
   `cd ~/team14
   source venv/bin/activate`
4. nd clone this repository. We can then do `bash bash\pipeline.sh`.
