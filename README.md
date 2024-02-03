# OpenSanctions Dashboard

This project aims to create a dashboard that enables a visual analysis of 
inter-countries sanctions from a business perspective 
using the [OpenSanctions Default](https://www.opensanctions.org/datasets/default/) dataset from [OpenSanctions](https://www.opensanctions.org/datasets/default/). 

Additionally, the [7+ Million Company Dataset](https://www.kaggle.com/datasets/peopledatalabssf/free-7-million-company-dataset) 
from [People Data Labs](https://www.peopledatalabs.com/) is joined in order to include affected industries. 

## Installation

* Install [PostgreSQL](https://www.postgresql.org/) & [Python](https://www.python.org/)
* Download Files into a `data` folder (replace &lt;YYYYMMDD> with the current data)
  * https://data.opensanctions.org/datasets/<YYYYMMDD>/default/index.json
  * https://data.opensanctions.org/datasets/<YYYYMMDD>/default/entities.ftm.json
  * https://www.kaggle.com/datasets/peopledatalabssf/free-7-million-company-dataset
* create virtual env and install the requirements
  ```bash 
    python3 -m venv .venv
    source .venv/bin/activate
    pip2 install -r requirements.txt
  ```
* Create database and insert the data (+transformations)
  * Create database 
    ```bash 
    sudo -u postgres psql 
    ``` 
    In the PostgreSQL editor insert the following SQL code:
    ```SQL
      CREATE DATABASE sanctions; 
      ALTER DATABASE sanctions OWNER TO sanctions;
      CREATE USER sanctions WITH ENCRYPTED PASSWORD 'sanctions';
      ALTER DATABASE sanctions OWNER TO sanctions;
      GRANT ALL PRIVILEGES ON DATABASE sanctions to sanctions;
      GRANT ALL ON SCHEMA sanctions.public TO sanctions;
    ```
  * Create schema
    ```bash
    python3 DB.py sql/schema.sql
    ```
  * Insert the OpenSanctions entries/ datasets and companies in the database (extract schemas & industries)
    ```bash
    # Insert the OpenSanctions entries & datasets in the database and extract the schemas
    python3 .\util\ParseOpenSanctionsData.py download_datasets data/index.json
    python3 .\util\ParseOpenSanctionsData.py write_entities data/entities.ftm.json
    python3 .\util\ParseOpenSanctionsData.py extract_schemas data/schemas.txt
  
    # Insert the CompanyData in the database and extract the industries
    python3 .\util\ParserCompanySetData.py parser_company_set_data data/companies_sorted.csv
    python3 .\util\ParserCompanySetData.py extract_industries data/industries.txt
    ```
  * Insert the countries and perform the SQL transformations to enable the `Country-Sanctions->country` analysis.
    Furthermore, it adds indexes to increase the dashboards performance. 
    ```bash
    python3 DB.py sql/countries.sql
    python3 DB.py sql/index_and_joins.sql
    ```
* Start the Dashboard:
  ```bash
  python3 sanctions_dashboard dashboard.py
  ```
  
## Disclaimer
This dashboard was created by TU Vienna student [Nicolas Bschor](https://github.com/HackerBschor) in collaboration 
with WU Vienna Professor [Dr. Jakob Müllner](https://www.wu.ac.at/iib/iib/faculty/muellner/) during the 
'Interdisciplinary Project in Data Science' course.

The data used in this dashboard is sourced from [OpenSanctions](https://www.opensanctions.org/) (Sanctions Information) 
and [People Data Labs](https://www.peopledatalabs.com/) (Company Industries). 
We only applied various transformation techniques in order to allow the analysis without changing the information. 
Therefore, we cannot ensure completeness, correctness, or if the data is up-to-date.
Users are advised to verify information independently, and the developers assume no responsibility for the 
consequences of its use. Use the dashboard at your own risk.