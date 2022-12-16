import pandas as pd
import gcsfs
import logging


# Reading csv file from GCS bucket 'abdu_processing'
fs = gcsfs.GCSFileSystem(project='sadaindia-tvm-poc-de')
with fs.open('abdu_processing/FINAL_DEC.csv') as f:
    logging.info("reading csv from gcs")
    df = pd.read_csv(f)

# Defining the data type validation function for MASKING table as 'mask_all'
def mask_all():

    filename1 = "NEW_DEC_JOINED.csv"
    Email_Client= "Email_x"
    Email_employee= "Email_y"
    Phone_Client="Phone_No"
    Phone_employee="PhoneNo"
    Project_username="Project_Username"
    Project_Internal="Project_Internal_URL"
    column_dataset1 = pd.read_csv(filename1)
    column_dataset1[Email_Client]="*******"+ column_dataset1[Email_Client].str[8:]
    column_dataset1[Email_employee]="*******"+ column_dataset1[Email_Client].str[8:]
    column_dataset1[Phone_Client]=column_dataset1[Phone_Client].str[0:1]+"XXXXXXX"+column_dataset1[Phone_Client].str[-1]
    column_dataset1[Phone_employee]=column_dataset1[Phone_employee].str[0:1]+"XXXXXXX"+column_dataset1[Phone_employee].str[-1]
    column_dataset1[Project_username]=column_dataset1[Project_username].str[0:1]+"XXXXXXX"+column_dataset1[Phone_employee].str[-1]
    column_dataset1[Project_Internal]="##.###.###.###"

    column_dataset1.to_csv('Final_Masked_table.csv')

