# DAG in Airflow is a Python script that contains a set of tasks and their dependencies
from airflow import DAG
from datetime import datetime
# PythonOperator is used to execute Python callables
from airflow.operators.python import PythonOperator
# GCSToGCSOperator copies objects from a bucket to another
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

#Importing source code for Validation 1 : Data entries
from abdu_emp_VAL1 import *
from abdu_proj_VAL1 import *
from abdu_client_VAL1 import *

# Importing source code for Validation 2 : Data type
from abdu_emp_VAL2 import *
from abdu_proj_VAL2 import *
from abdu_client_VAL2 import *

# Importing source code for Encryption
from abdu_emp_enc import *
from abdu_proj_enc import *
from abdu_client_enc import *

# Importing source code for Joining Tables
from abdu_join import *



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

###############################################################################################################

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

    # JOINING TABLES : EMPLOYEE TABLE + PROJECT TABLE + CLIENT TABLE
    join_tables = PythonOperator(
        task_id='join_tables',
        python_callable=join_tables,
        dag=dag
    )

###############################################################################################################




# DAG TASK FLOW

cp_emp_table >> val_1_emp_table
cp_proj_table >> val_1_proj_table
cp_client_table >> val_2_client_table

val_1_emp_table >> val_2_emp_table
val_1_proj_table >> val_2_proj_table
val_2_client_table >> val_2_client_table

val_2_emp_table >> enc_emp_table
val_2_proj_table >> enc_proj_table
val_2_client_table >> enc_client_table

[enc_emp_table, enc_proj_table, enc_client_table] >> join_tables

