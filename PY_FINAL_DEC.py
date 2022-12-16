from cryptography.fernet import Fernet
import pandas as pd

############################################################################################################################################################################
def load_key():
    # opening the encrypted file
    return open("/FINAL/MAIN_KEY.key", "rb").read()

key = load_key()

# using the key
f = Fernet(key)
############################################################################################################################################################################
df=pd.read_csv("/FINAL/Final_Table.csv")
############################################################################################################################################################################
df['Empl_Eml'] = df['Empl_Eml'].apply(
    lambda x:
        f.decrypt(x).decode()
)

df['PhoneNo'] = df['PhoneNo'].apply(
    lambda x:
        f.decrypt(x).decode()
)
############################################################################################################################################################################
df['Project_Internal_URL'] = df['Project_Internal_URL'].apply(
    lambda x:
        f.decrypt(x).decode()
)

df['Project_Username'] = df['Project_Username'].apply(
    lambda x:
        f.decrypt(x).decode()
)
############################################################################################################################################################################
df['Client_Eml'] = df['Client_Eml'].apply(
    lambda x:
        f.decrypt(x).decode()
)

df['Client_Phone_No'] = df['Client_Phone_No'].apply(
    lambda x:
        f.decrypt(x).decode()
)
############################################################################################################################################################################
# opening the file in write mode and
#writing the csv file with decrypted data as new csv file
df.to_csv("/Users/abdurahman.salim/PycharmProjects/use_case/FINAL/FINAL_DEC.csv")
