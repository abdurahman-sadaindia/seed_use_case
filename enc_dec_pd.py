from cryptography.fernet import Fernet
import pandas as pd
import csv
df=pd.read_csv("../../use_case/csv_files/source_files/Employee_table.csv",header=0,skiprows = 1)

# key generation
key = Fernet.generate_key()

# string the key in a file
with open('../../use_case/encryption/keys/emp_key.key', 'wb') as filekey: # wb is write byte
    filekey.write(key)
#filekey.key is the file that contains the encryption key which is used to encrypt and decrypt the csv file

# using the generated key
f = Fernet(key)

df['Email'] = df['Email'].apply(
    lambda x:
        f.encrypt(x.encode())
)

df['PhoneNo'] = df['PhoneNo'].apply(
    lambda x:
        f.encrypt(x.encode())
)

#writing the csv file with encrypted data as new csv file
df.to_csv("../../use_case/csv_files/val_enc_files/emp_enc.csv")
