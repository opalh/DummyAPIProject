# Dummy API project 
This script store's data from a dummy API https://dummyapi.io/docs into a MySQL database.

---
**Important note**: Change the configurations' info in db_connection/connection_info.py file before running this script.

- hostname
- username
- password
- dbname default value ("learnWorlds")

---
### Data Analysis with MySQL 

Once the Python script is completed, test the data with the queries in *data_analysis_queries.sql* file.

---

### Orchestration & Monitoring

For ease deploying to production we could create a docker image with the application.

To go one step further, we could enhance this application by using Google's platform's tools Cloud Composer (Apache Airflow) for the orchestration, Big Query as the data warehouse, and Data Studio for creating a dashboard with some insights. 
