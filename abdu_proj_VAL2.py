# Pandas is a Python library used for working with data sets.
# Functions : analyzing, cleaning, exploring, and manipulating data
# Pandas allows us to analyze big data and make conclusions
# Pandas can clean messy data sets, and make them readable and relevant
# PandasSchema is a module for validating tabulated data, such as CSVs (Comma Separated Value files), and TSVs (Tab Separated Value files)
# PandasSchema uses the incredibly powerful data analysis tool Pandas
import pandas as pd
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation
# gcsfs is a pythonic file-system interface to Google Cloud Storage
from google.cloud import storage
import gcsfs
import logging

# Reading csv file from GCS bucket 'abdu_processing'
fs = gcsfs.GCSFileSystem(project='sadaindia-tvm-poc-de')
with fs.open('abdu_processing/val_1_proj.csv') as f:
    logging.info("reading csv from gcs")
    df = pd.read_csv(f)

# Defining the data type validation function for project table as 'val_2_proj'
def val_2_proj():
    def check_string(string):
        try:
            str(string)
        except Exception:
            return False
        return True

    def check_int(number):
        try:
            int(number)
        except ValueError:
            return False
        return True



    # Validation of Integer
    logging.info("integer validation")
    int_validation = [CustomElementValidation(lambda i: check_int(i), 'datatype is not an integer')]

    # Validation of String
    logging.info("string validation")
    string_validation = [CustomElementValidation(lambda s: check_string(s), 'datatype is not string')]


    # Checking the datatype of Schema
    logging.info("schema check")

    schema = pandas_schema.Schema([
        Column('Project_ID', int_validation),
        Column('Emp_id', int_validation),
        Column('Client_ID', int_validation),
        Column('ProjectName', string_validation),
        Column('Proj_Start_Dt', string_validation),
        Column('Proj_End_Dt', string_validation),
        Column('Project_Cost', string_validation),
        Column('Project_Domain', string_validation),
        Column('Project_Internal_URL', string_validation),
        Column('Project_Username', string_validation),
        Column('Project_tag', string_validation),
        Column('Project_Owner', string_validation),
        Column('Project_Time_Zone', string_validation),
        Column('Project_Contact', string_validation),
        Column('Project_Description', string_validation),
    ])

    # Checking for errors in Schema
    logging.info("checking errors")
    errors = schema.validate(df)
    # Finding the error rows
    errors_index_rows = [e.row for e in errors]
    # Remove the error rows and store the remaining as data_clean
    data_clean = df.drop(index=errors_index_rows)
    logging.info("creating error and valid csv")

    # Locally writing the clean data to a temporary file 'val_2_proj_clean.csv'
    data_clean.to_csv('val_2_proj_clean.csv', mode='w', index=False)

    # Locally writing the error data into a temporary file 'inv_2_proj_error.csv'
    pd.DataFrame({"error": errors}).to_csv('inv_2_proj_error.csv', mode='w', index=False)


    storage_client = storage.Client()
    # Creating empty csv file in Cloud bucket "abdu_processing" to load valid data
    valid_bucket = storage_client.bucket("abdu_processing")
    valid_blob = valid_bucket.blob("val_2_proj.csv")
    # Creating empty csv file in Cloud bucket "abdu_error" to load invalid data
    invalid_bucket = storage_client.bucket("abdu_error")
    invalid_blob = invalid_bucket.blob("inv_2_proj.csv")





    # Uploading the clean data from 'val_2_proj_clean.csv' to 'val_2_proj.csv'
    logging.info("uploading csv")
    valid_blob.upload_from_filename("val_2_proj_clean.csv")

    # Uploading the error data from 'inv_2_proj_error.csv' to 'inv_2_proj.csv'
    logging.info("uploading error data into csv file")
    invalid_blob.upload_from_filename("inv_2_proj_error.csv")