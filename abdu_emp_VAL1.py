import csv
# StringIO module is an in-memory file-like object
# This object can be used as input or output to the most function
# that would expect a standard file object
from io import StringIO
from google.cloud import storage


def gcp_csv_to_df():
    storage_client = storage.Client()
    bucket = storage_client.bucket("abdu_processing")
    blob = bucket.blob("cp_emp.csv")
    data = blob.download_as_string()
    data = data.decode("utf-8")
    data = StringIO(data)
    return data

# Defining the validation function for data entries of employee table as 'val_1_emp'
def val_1_emp():
    input_csv = gcp_csv_to_df()
    storage_client = storage.Client()
    valid_bucket = storage_client.bucket("abdu_processing")
    blob_valid = valid_bucket.blob("val_1_emp.csv")
    invalid_bucket = storage_client.bucket("abdu_error")
    blob_invalid = invalid_bucket.blob("inv_1_emp.csv")

    with blob_invalid.open("w") as inv:
        with blob_valid.open("w") as val:
            inv_writer = csv.writer(inv, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            val_writer = csv.writer(val, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_reader = csv.reader(input_csv, delimiter=",")

            count = 0
            for row in csv_reader:
                if count == 0:
                    _header = row
                    val_writer.writerow(_header)
                    count = 1
                else:
                    if len(row) != len(_header):
                        inv_writer.writerow(row)
                    else:
                        val_writer.writerow(row)
