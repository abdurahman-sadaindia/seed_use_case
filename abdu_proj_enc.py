from airflow.models import Variable
# Cryptography package is used to encrypt and decrypt data
# The fernet module of the cryptography package has inbuilt functions:
    # for the generation of the key,
    # encryption of plaintext into ciphertext, and
    # decryption of ciphertext into plaintext using
    # encrypt and decrypt methods
from cryptography.fernet import Fernet
# Pandas is a Python library used for working with data sets.
# Functions : analyzing, cleaning, exploring, and manipulating data
# Pandas allows us to analyze big data and make conclusions
# Pandas can clean messy data sets, and make them readable and relevant
# PandasSchema is a module for validating tabulated data, such as:
    # CSVs (Comma Separated Value files), and
    # TSVs (Tab Separated Value files)
# PandasSchema uses the incredibly powerful data analysis tool Pandas
import pandas as pd
# gcsfs is a pythonic file-system interface to Google Cloud Storage
import gcsfs
from google.cloud import storage
import logging

# Using the key stored in the airflow UI
logging.info("fetching the value of the encryption key stored in airflow UI")
fs = gcsfs.GCSFileSystem(project='sadaindia-tvm-poc-de')

# Reading the csv file from GCS bucket
logging.info("reading from gcs bucket")
with fs.open('abdu_processing/val_2_proj.csv') as val_proj:
    logging.info("reading valid project csv from gcs")
    df1 = pd.read_csv(val_proj)
    logging.info("reading from gcs bucket")

# Defining the encryption function for project table as 'enc_proj'
def enc_proj():

    key = Variable.get("SECRET_ENCRYPTION_KEY")
    f = Fernet(key)
    logging.info("passing the encryption key into f")

    storage_client = storage.Client()
    # Creating empty csv file in Cloud bucket "abdu_processing" to load encrypted data
    bucket = storage_client.bucket("abdu_processing")
    employee_encrypt_blob = bucket.blob("enc_proj.csv")
    logging.info("Creating csv file in gcs bucket")

    df1['Project_Internal_URL'] = df1['Project_Internal_URL'].apply(
        lambda x:
        f.encrypt(x.encode()).decode()
    )
    df1['Project_Username'] = df1['Project_Username'].apply(
        lambda x:
        f.encrypt(x.encode()).decode()
    )

    # Locally writing the encrypted data to a temporary file 'enc_proj_temp.csv'
    logging.info("writing encrypted data into temporary project csv file")
    df1.to_csv("enc_proj_temp.csv")

    # Uploading the error data from 'enc_proj_temp.csv' to 'enc_proj.csv'
    logging.info("uploading encrypted data into empty project csv file created in GCS bucket")
    employee_encrypt_blob.upload_from_filename("enc_proj_temp.csv")