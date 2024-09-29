# POC3_pyspark


## Overview

This project involves using PySpark to read, filter, and process client and financial data from CSV files. The main goal is to filter client data based on country names, join it with financial data, and output the results into a specified directory. The project also includes test cases for validating the filtering and renaming functions. The PySpark application can be run locally on WSL (Windows Subsystem for Linux) using Spark and Python.

The data processing pipeline is managed using command-line arguments for flexibility, allowing different datasets and country filters to be applied without changing the code.
## Tech Stack

    PySpark: Framework for distributed data processing.
    Python: Main programming language for the application.
    SparkSession: Entry point for Spark functionality.
    Log4j: Logging framework integrated with PySpark for application logging.
    WSL (Windows Subsystem for Linux): Local environment used to run Spark and Python.
    Chispa: PySpark testing library for comparing DataFrames.

 
## Running the Application Locally
spark-submit --master  spark://XXXXXXXX \
/home/wojkamin/my_folder/poc3_folder/poc3.py \
--clients_csv poc3_dataset/clients.csv \
--financial_csv "poc3_dataset/financial.csv" \
--list_countries "Poland" "France"

or

python poc3.py \
--clients_csv poc3_dataset/clients.csv \
--financial_csv "poc3_dataset/financial.csv" \
--list_countries "Poland" "France"

results are stored in folder client_data
