# DAG in Airflow is a Python script that contains a set of tasks and their dependencies
from airflow import DAG
from datetime import datetime
# PythonOperator is used to execute Python callables
from airflow.operators.python import PythonOperator
# GCSToGCSOperator copies objects from a bucket to another
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
# GCSToBigQueryOperator loads objects from a bucket to bigquery
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# #Importing source code for Validation 1 : Data entries
from abdu_emp_VAL1 import *
from abdu_proj_VAL1 import *
from abdu_client_VAL1 import *
#
# Importing source code for Validation 2 : Data type
from abdu_emp_VAL2 import *
from abdu_proj_VAL2 import *
from abdu_client_VAL2 import *

# Importing source code for Encryption
from abdu_emp_enc import *
from abdu_proj_enc import *
from abdu_client_enc import *

# # Importing source code for Joining Tables
from abdu_join import *

#from BQ_SQL_TR import *
# Importing source code for Decryption


# Importing source code for Masking

with DAG("abdu_dag", start_date=datetime(2022, 12, 12), catchup=False, schedule_interval=None) as dag:
    #catchup false means it will not run pending dags from start date

###############################################################################################################

    # MAKE COPY OF EMPLOYEE TABLE
    cp_emp_table = GCSToGCSOperator(
        task_id='cp_emp_table',
        source_bucket='abdu_to_process',
        source_object='emp_source_table.csv',
        destination_bucket='abdu_processing',
        destination_object='cp_' + 'emp.csv',
    )

    # MAKE COPY OF PROJECT TABLE
    cp_proj_table = GCSToGCSOperator(
        task_id='cp_proj_table',
        source_bucket='abdu_to_process',
        source_object='proj_source_table.csv',
        destination_bucket='abdu_processing',
        destination_object='cp_' + 'proj.csv',
    )
    # MAKE COPY OF CLIENT TABLE
    cp_client_table = GCSToGCSOperator(
        task_id='cp_client_table',
        source_bucket='abdu_to_process',
        source_object='client_source_table.csv',
        destination_bucket='abdu_processing',
        destination_object='cp_' + 'client.csv',
    )
###############################################################################################################

    # VALIDATION FOR DATA ENTRIES : EMPLOYEE TABLE
    val_1_emp_table = PythonOperator(
        task_id='val_1_emp_table',
        python_callable=val_1_emp,
        dag = dag
    )

    # VALIDATION FOR DATA ENTRIES : PROJECT TABLE
    val_1_proj_table = PythonOperator(
        task_id='val_1_proj_table',
        python_callable=val_1_proj,
        dag = dag
    )

    # VALIDATION FOR DATA ENTRIES : CLIENT TABLE
    val_1_client_table = PythonOperator(
        task_id='val_1_client_table',
        python_callable=val_1_client,
        dag=dag
    )

# ###############################################################################################################

    # VALIDATION FOR DATA TYPE : EMPLOYEE TABLE
    val_2_emp_table = PythonOperator(
        task_id='val_2_emp_table',
        python_callable=val_2_emp,
        dag = dag
    )

    # VALIDATION FOR DATA TYPE : PROJECT TABLE
    val_2_proj_table = PythonOperator(
        task_id='val_2_proj_table',
        python_callable=val_2_proj,
        dag=dag
    )

    # VALIDATION FOR DATA TYPE : CLIENT TABLE
    val_2_client_table = PythonOperator(
        task_id='val_2_client_table',
        python_callable=val_2_client,
        dag=dag
    )

###############################################################################################################

    # ENCRYPTION OF EMPLOYEE TABLE
    enc_emp_table = PythonOperator(
        task_id='enc_emp_table',
        python_callable=enc_emp,
        dag=dag
    )

    # ENCRYPTION OF PROJECT TABLE
    enc_proj_table = PythonOperator(
        task_id='enc_proj_table',
        python_callable=enc_proj,
        dag=dag
    )

    # ENCRYPTION OF CLIENT TABLE
    enc_client_table = PythonOperator(
        task_id='enc_client_table',
        python_callable=enc_client,
        dag=dag
    )
###############################################################################################################
    # Loading employee base table to Big query
    bq_load_emp_base = GCSToBigQueryOperator(
        task_id='bq_load_emp_base',
        bucket="abdu_to_process",
        source_objects=["emp_source_table.csv"],
        source_format='CSV',
        destination_project_dataset_table="sadaindia-tvm-poc-de.abdu_dataset.employee_base_table",
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )
    # Loading project base table to Big query
    bq_load_proj_base = GCSToBigQueryOperator(
        task_id='bq_load_proj_base',
        bucket="abdu_to_process",
        source_objects=["proj_source_table.csv"],
        source_format='CSV',
        destination_project_dataset_table="sadaindia-tvm-poc-de.abdu_dataset.project_base_table",
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )
    # Loading client base table to Big query
    bq_load_client_base = GCSToBigQueryOperator(
        task_id='bq_load_client_base',
        bucket="abdu_to_process",
        source_objects=["client_source_table.csv"],
        source_format='CSV',
        destination_project_dataset_table="sadaindia-tvm-poc-de.abdu_dataset.client_base_table",
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )

###############################################################################################################

    # JOINING TABLES : EMPLOYEE TABLE + PROJECT TABLE + CLIENT TABLE
    join_tables = PythonOperator(
        task_id='join_tables',
        python_callable=join_tables,
        dag=dag
     )

###############################################################################################################
    # Create Empty dataset in BQ
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id='abdu_dataset',
        project_id='sadaindia-tvm-poc-de',

    )

    # Loading Joined encrypted table from GCS to Big query
    load_staging_table = GCSToBigQueryOperator(
        task_id='load_staging_table',
        bucket="abdu_processing",
        source_objects=["joined_table_bq_load.csv"],
        source_format='CSV',
        destination_project_dataset_table="sadaindia-tvm-poc-de.abdu_dataset.Employee_staging_table",
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )
    # Data Transformation in BQ
    data_trans_query_job = BigQueryOperator(
        task_id='data_trans_query_job',
        use_legacy_sql=False,
        location="US",
        sql='bq_trans_query.sql'
    )

# ###############################################################################################################
#     # DECRYPTION OF FINAL TABLE
#     decryption = PythonOperator(
#         task_id='decryption',
#         python_callable=dec_all,
#         dag=dag
#      )
#
# ###############################################################################################################
#     # MASKING  OF FINAL TABLE
#     masking = PythonOperator(
#         task_id='masking',
#         python_callable=mask_all,
#         dag=dag
#      )
#
#
###############################################################################################################
    # Loading the Final_table_To_be_automated from BQ to GCS
    bq_to_gcs_load_final_csv = BigQueryToGCSOperator(
        task_id='bq_to_gcs_load_final_csv',
        source_project_dataset_table=f"sadaindia-tvm-poc-de.abdu_to_be_automated.Final_table_To_be_automated",
        destination_cloud_storage_uris=["gs://abdu_processed/Employee_Final_table_To_be_automated.csv"],
        export_format='CSV',
        field_delimiter=',',
        print_header=True,
    )
#DAG TASK FLOW

cp_emp_table >> val_1_emp_table
cp_proj_table >> val_1_proj_table
cp_client_table >> val_1_client_table

val_1_emp_table >> val_2_emp_table
val_1_proj_table >> val_2_proj_table
val_1_client_table >> val_2_client_table

val_2_emp_table >> enc_emp_table
val_2_proj_table >> enc_proj_table
val_2_client_table >> enc_client_table

[enc_emp_table,enc_proj_table,enc_client_table] >> create_dataset

create_dataset >> [bq_load_emp_base,bq_load_proj_base,bq_load_client_base]

[bq_load_emp_base, bq_load_proj_base, bq_load_client_base] >> join_tables

join_tables >> load_staging_table >> data_trans_query_job

#data_trans_query_job >> decryption >> masking

data_trans_query_job >> bq_to_gcs_load_final_csv