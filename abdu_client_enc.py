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
with fs.open('abdu_processing/val_2_client.csv') as val_client:
    logging.info("reading valid client csv from gcs")
    df1 = pd.read_csv(val_client)
    logging.info("reading from gcs bucket")

# Defining the encryption function for client table as 'enc_client'
def enc_client():

    key = Variable.get("ABDU_ENCRYPTION_KEY")
    f = Fernet(key)
    logging.info("passing the encryption key into f")

    storage_client = storage.Client()
    # Creating empty csv file in Cloud bucket "abdu_processing" to load encrypted data
    bucket = storage_client.bucket("abdu_processing")
    employee_encrypt_blob = bucket.blob("enc_client.csv")
    logging.info("Creating csv file in gcs bucket")

    df1['Email'] = df1['Email'].apply(
        lambda x:
        f.encrypt(x.encode()).decode()
    )
    df1['Phone_No'] = df1['Phone_No'].apply(
        lambda x:
        f.encrypt(x.encode()).decode()
    )

    # Locally writing the encrypted data to a temporary file 'enc_client_temp.csv'
    logging.info("writing encrypted data into temporary client csv file")
    df1.to_csv("enc_client_temp.csv")

    # Uploading the error data from 'enc_client_temp.csv' to 'enc_client.csv'
    logging.info("uploading encrypted data into empty client csv file created in GCS bucket")
    employee_encrypt_blob.upload_from_filename("enc_client_temp.csv")
