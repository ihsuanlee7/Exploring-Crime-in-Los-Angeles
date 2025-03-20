# Repository for Team 14

## Datasets
For this analysis we used the following datasets:

Listing of All Businesses (260MB): [link](https://data.lacity.org/Administration-Finance/Listing-of-All-Businesses/r4uk-afju/about_data)<br>
A dataset containing businesses located in Los Angeles, including business name, address, basic business description, start & end date, etc. (Updated Feb 2025)

Crime Data from 2020 to Present (248MB): [link](https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8/about_data)<br>
A dataset containing records of reported crimes in Los Angeles, including date, location(Latitude and Longitude), crime type, crime detail, etc. (Updated Feb 2025)

## Pipeline logic
The pipeline automates downloading the two large dataset and running the PySpark and DuckDB scripts. This process outputs `.csv` files that are used to populate the Tableau dashboard.

## Pipleline automation(running your ETL pipeline locally):
1. Clone this repository (only if you have not already): `git clone https://github.com/Prof-Rosario-UCLA/team14.git`, but you really only need the content in final_405 directory
2. Enter the team directory: `cd cd team14/final_405`
3. Download the data from datasets section, you can find the data we use in the folder: [link](https://drive.google.com/drive/folders/1I45w8szj2sQVF1WZzX8srBmxEEFmLCBz?usp=sharing)
4. Modify etl.py's TODO section to your working directory
5. Pull the latest changes (if needed): `git pull origin main`
6. download required packages with the following command in terminal: pip install -r requirements.txt
7. Install Java and Set Java Environment Variable with the command: export JAVA_HOME=/path/to/your/java
8. Run the script with: python your_script.py

   
## Pipeline automation
1. Connect to your EC2 instance.
2. Clone this repository (only if you have not already): `git clone https://github.com/Prof-Rosario-UCLA/team14.git`
3. Enter the team directory: `cd team14`
4. Pull the latest changes (if needed): `git pull origin main`
5. Activate the virtual environment: `source venv/bin/activate`
6. Run the pipeline: `bash bash/pipeline.sh`

## Data Visualization
Please view our Tableau dashboard through this [Link](https://public.tableau.com/app/profile/i.hsuan.lee6901/viz/405book/Dashboard1?publish=yes)
