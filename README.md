# Data Lake to assess the economic impact of Covid19
***
After the coronavirus outbreak (COVID-19) governments, companies and organizations around the world are working to help prevent and alleviate the effects of coronavirus and also face its consequences. 

Unfortunately, this virus has had an impact in very varied areas. This repository collects information to assess the _economic impact_ of the Covid-19 in the stock market in more than 1850 companies in 50 different countries around the world.

To do that, this repository includes the script to produce a dataset of more than 2.5 million rows with the stock market data of each company that dates back 400 days, which will allow to clearly see the values before the outbreak and the impact during the pandemic.  
This repository also includes a file produced by the **Oxford Coronavirus Goverment Response Tracker (OxCGRT project)** which contains information of each country with a _Government Response Stringency Index_ which indicates the stringency regarding each government's response to reduce the impacts of covid-19 (https://ourworldindata.org/policy-responses-covid#schools-closures).

These two files correspond to the staging tables:
![alt text](https://raw.githubusercontent.com/Gares95/Data-Engineering-Nanodegree-Capstone-Project/master/data/Img/Staging_tables.PNG)

After cleaning and processing the data into analytics tables using Spark in a **EMR cluster** (through the ETL file) we will obtain the Data Lake desired in a S3 bucket.  

This repository includes the files necessary to:
- Generate a huge dataset with stock market values of each company using _yfinance_ library (https://github.com/ranaroussi/yfinance).
- Build an ETL pipeline for a *Data Lake* hosted on S3 and a database with a star schema model. 
- Do quality checks on the Data Lake to ensure it has the right data and structure.
- An example as a presentation of how this data can be used to analyze the economic impact related to the stringency of each government.

The reason to use a Data Lake for this model is due to its simple implementation in S3 with a flexible storage system that allow our program to function properly regardless the data processed and uploaded.
The reason for an EMR cluster with Spark is its capability to run big data frameworks and its integrity to process and analyze big amounts of data.

# Data Files
***
The datasets used for this project are:
- Stock_Countries.csv: This file contains information of each company, including:
    - Name
    - Sector
    - Ticker
    - Country
- Covid_Stringency_Index: This file contains the Stringency Index of each country regarding the measures taken to fight covid-19.
- countrycodesA3.csv

With the previous files we will produce the staging tables
- Staging_stock_table.csv: To produce this file I've used a filed with information about each country that I've collected (Stock_Countries.csv) and through each company's _Ticker_ I've extracted the stock market values using _yfinance_ library (https://github.com/ranaroussi/yfinance). 
- Staging_stringency_table.csv: This file comes from the original _Covid_Stringency_Index_ expanding it date range to match the start of _Staging_stock_table_.


# Data Lake Star Schema
The tables of the star schema that is going to be created using this program are:

- _Fact table_:
1. Economic_status_table [ec_status_id, Date, country_code, stringency_index, stock_id, value_type, value]

- _Dimension tables_:
2. time_table [Date, day, month, year, week_day]
3. Country_table [country_code, country_name]
4. Company_table [stock_Id, company_name, sector]
5. time [start_time, hour, day, week, month, year, weekday]


The final database with a star schema model will have the next structure:
![alt text](https://raw.githubusercontent.com/Gares95/Data-Engineering-Nanodegree-Capstone-Project/master/data/Img/Data_model.PNG)

# EMR cluster
The AWS infrastructure used was an EMR Cluster with the following settings:  
Software configuration:
- Release: emr-5.31.0 
- Applications: Spark: Spark 2.4.6 on Hadoop 2.10.0 YARN and Zeppelin 0.8.2
Hardware configuration:
- Instance type: m5.xlarge
- Number of instances: 3
Using an EC2 key pair for security and access.

# Requirements
For the _Cleaning_Staging_Data_ notebook to work it is necessary to install the library _yfinance_. You can use:
``pip install -r requirements.txt``

# Program files
***
## Cp.cfg

This file contains the AWS credentials to access the S3 buckets. 
Here you will have to introduce your AWS key and secret access key:

[AWS]

AWS_ACCESS_KEY_ID= _your AWS access key_
    
AWS_SECRET_ACCESS_KEY= _your AWS secret access key_

## Cleaning_Staging_Data.ipynb

This first script serves to explore the data and fix any quality issues and also to produce and load to an S3 bucket the staging tables that are mentioned above. 
**Note: It is necessary to run this script before running Etl.py**
    
## Etl.py

With this file we will process all files from the S3 buckets and create the star schema (with all the tables mentioned above) and load them back into the S3 which will act as our Data Lake. 

## Quality_Check_And_Example.ipynb

This file can be used to perform quality checks and it presents an example of how the data lake can be used to analyze the economic impact of covid-19. 
    
## README.md

This file provides the description of the program and process of the etl.

# Possible Future Scenarios
***
Because the data lake is subject to possible modifications, I'm going to suggest some solutions:  
- The data increases by 100X:
For this scenario the right approach would be to increase the number of instances of the EMR cluster and maybe upgrade the Instance type to a _r4.xlarge_ to take advantage of the increased memory.
- Pipelines run on daily basis at 7am:
In this case it would be recommended to incorporated Airflow to create a Dag with a schedule that would run the _Cleaning_Staging_Data_ to obtain the most recent values of the stock market values of the companies in our Stock_Countries.csv file and the ETL to update the data lake.
- The database is needed to be accessed by 100+ people:
Thanks to the S3 our data lake would be accessible to the extra 100 people but if necessary, we could implement it to use Redshift with auto-scaling instead for a better aproach.

### Credits
Udacity provided the template and the guidelines to start this project.
The completion of this was made by Guillermo Garcia and the review of the program and the verification that the project followed the proper procedures was also made by my mentor from udacity.

