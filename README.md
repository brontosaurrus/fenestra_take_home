# Fenestra Take Home Task

## Instructions:

1. Pull repo locally.

2. Add your own environment variables in line with `environment.py` naming.

3. Run `python3 main.py` to pull any new files, push to sql and check for any duplicates.


## Questions:
**1.Check for new files and download them from https://console.cloud.google.com/storage/browser/fst-python-case-study (during the code review, we will upload a new file to the bucket and see how your code copes)**

This is done through the GCS_Handler class. It checks against a created schema `processed_files` for files that have already been processed. 


**2. Insert these files into the Cloud SQL Postgres database with details provided below. You will need to create a new table with a suitable schema. For schema hints, see https://support.google.com/admanager/table/7401123?hl=en&ref_topic=3038164**

A schema has been created in line with the data retrieved from the bucket. The primary key group has been chosen (["OrderId", "LineItemId", "KeyPart"]). 

**3. Check and remove duplicates (you can do this before uploading if you prefer)**

To check for duplicates all current records in the schema are pulled and checked against the new data. They are checked for duplicates against the three primary keys chosen. They are then pushed back to a 'tmp' table before switching the naming of the tables to the original. A cleanup is then performed. 

**4. Answer the following questions:**

The output of these questions come when running the script. 


a. How many records are there per day and per hour?

b. What is the total of the EstimatedBackFillRevenue field per day and per hour

c. How many records and what is the total of the EstimatedBackFillRevenue field per Buyer?

d. List the unique Device Categories by Advertiser.

e. How many duplicate rows were there?

**NOTE: Ideally would have loved to integrate pyspark into the solution but with my current setup couldn't do so. I reduced dataframes for testing. Have a rough Spark handler file that is not used, just for some free writing coding for my own benefit.**
