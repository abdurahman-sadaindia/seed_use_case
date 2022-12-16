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
with fs.open('abdu_processing/val_1_emp.csv') as f:
    logging.info("reading csv from gcs")
    df = pd.read_csv(f)

# Defining the data type validation function for employee table as 'val_2_emp'
def val_2_emp():

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

    def check_float(float):
        try:
            int(float)
        except ValueError:
            return False
        return True

    # Validation of Integer
    logging.info("integer validation")
    int_validation = [CustomElementValidation(lambda i: check_int(i), 'datatype is not an integer')]

    # Validation of String
    logging.info("string validation")
    string_validation = [CustomElementValidation(lambda s: check_string(s), 'datatype is not string')]

    # Validation of float
    logging.info("float validation")
    float_validation = [CustomElementValidation(lambda f: check_float(f), 'datatype is not float')]

    # Checking the datatype of Schema
    logging.info("schema check")

    schema = pandas_schema.Schema([
        Column('Emp_id', int_validation),
        Column('Empf_name', string_validation),
        Column('Empl_name', string_validation),
        Column('Joining_Date', string_validation),
        Column('Email', string_validation),
        Column('PhoneNo', string_validation),
        Column('Job_Title', string_validation),
        Column('Department', string_validation),
        Column('Language', string_validation),
        Column('City', string_validation),
        Column('State', string_validation),
        Column('Postal_Cd', string_validation),
        Column('Country_Cd', string_validation),
        Column('Latitude', float_validation),
        Column('Longitude', float_validation),
        Column('Time_Zone', string_validation),
        Column('Street_Add', string_validation),
        Column('Street_Nm', string_validation),
        Column('Street_suffix', string_validation),
        Column('Street_No', int_validation),
        Column('Salary', string_validation),
        Column('Expertise', string_validation),
        Column('Gender', string_validation),
        Column('Race', string_validation),
        Column('EIN_No', string_validation),
        Column('EmpDevice_Ip', string_validation),
        Column('EmpDevice_Mac', string_validation),
        Column('Emp_Prof', string_validation),
        Column('Buzzword', string_validation),
    ])

    # Checking for errors in Schema
    logging.info("checking errors")
    errors = schema.validate(df)
    # Finding the error rows
    errors_index_rows = [e.row for e in errors]
    # Remove the error rows and store the remaining as data_clean
    data_clean = df.drop(index=errors_index_rows)
    logging.info("creating error and valid csv")

    # Locally writing the clean data to a temporary file 'val_2_emp_clean.csv'
    data_clean.to_csv('val_2_emp_clean.csv', mode='w', index=False)

    # Locally writing the error data into a temporary file 'inv_2_emp_error.csv'
    pd.DataFrame({"error": errors}).to_csv('inv_2_emp_error.csv', mode='w', index=False)


    storage_client = storage.Client()
    # Creating empty csv file in Cloud bucket "abdu_processing" to load valid data
    valid_bucket = storage_client.bucket("abdu_processing")
    valid_blob = valid_bucket.blob("val_2_emp.csv")
    # Creating empty csv file in Cloud bucket "abdu_error" to load invalid data
    invalid_bucket = storage_client.bucket("abdu_error")
    invalid_blob = invalid_bucket.blob("inv_2_emp.csv")


    # Uploading the clean data from 'val_2_emp_clean.csv' to 'val_2_emp.csv'
    logging.info("uploading clean data into csv file")
    valid_blob.upload_from_filename("val_2_emp_clean.csv")

    # Uploading the error data from 'inv_2_emp_error.csv' to 'inv_2_emp.csv'
    logging.info("uploading error data into csv file")
    invalid_blob.upload_from_filename("inv_2_emp_error.csv")