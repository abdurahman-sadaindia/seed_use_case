# Pandas is a Python library used for working with data sets.
# Functions : analyzing, cleaning, exploring, and manipulating data
# Pandas allows us to analyze big data and make conclusions
# Pandas can clean messy data sets, and make them readable and relevant
# PandasSchema is a module for validating tabulated data, such as CSVs (Comma Separated Value files), and TSVs (Tab Separated Value files)
# PandasSchema uses the incredibly powerful data analysis tool Pandas
import pandas as pd
from google.cloud import storage


def join_tables():
    storage_client = storage.Client()
    bucket = storage_client.bucket("abdu_processing")
    # Reading the encrypted employee file and downloading
    blob = bucket.blob("enc_emp.csv")
    blob.download_to_filename("valid_employee_temp.csv")

    blob = bucket.blob("enc_proj.csv")
    blob.download_to_filename("valid_project_temp.csv")

    blob = bucket.blob("enc_client.csv")
    blob.download_to_filename("valid_client_temp.csv")

    df1 = pd.read_csv("valid_client_temp.csv")
    df2 = pd.read_csv("valid_employee_temp.csv")
    df3 = pd.read_csv("valid_project_temp.csv")

    project_client = df3.merge(df1,on=["Client_ID"])
    project_client_employee = project_client.merge(df2,on=["Emp_id"])
    #temp file locally created
    project_client_employee.to_csv("joined_table_temp.csv")

    joined_table_blob = bucket.blob("joined_table_final.csv")
    #final table
    joined_table_blob.upload_from_filename("joined_table_temp.csv")