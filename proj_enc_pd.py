from cryptography.fernet import Fernet
import pandas as pd

df=pd.read_csv("../../use_case/csv_files/source_files/project.csv",header=0,skiprows = 1)

# key generation
key = Fernet.generate_key()

# string the key in a file
with open('../../use_case/encryption/keys/proj_key.key', 'wb') as filekey: # wb is write byte
    filekey.write(key)
# proj_key.key is the file that contains the encryption key which is used to encrypt and decrypt the csv file

# using the generated key
f = Fernet(key)

df['Project_Internal_URL'] = df['Project_Internal_URL'].apply(
    lambda x:
        f.encrypt(x.encode())
)

df['Project_Username'] = df['Project_Username'].apply(
    lambda x:
        f.encrypt(x.encode())
)

#writing the csv file with encrypted data as new csv file
df.to_csv("../../use_case/csv_files/val_enc_files/proj_enc.csv")
