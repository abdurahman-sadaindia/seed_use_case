from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint()

from datetime import datetime

def _unni_model():
    return randint(1, 10)

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'task_1',
        'task_2',
        'task_3'
    ])
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'


with DAG("my-dag", start_date=datetime(2022, 11, 28)
                    schedule_interval="@daily", catchup=False) as dag;

                    task_1 = PythonOperator(
                        task_id="training_model_A"
                        python_callable=_unni_model
                    )

                    task_2 = PythonOperator(
                        task_id="training_model_B",
                        python_callable=_unni_model
                    )

                    task_3 = PythonOperator(
                        task_id="training_model_C",
                        python_callable=_unni_model
                    )

                    task_4 = BranchPythonOperator(
                        task_id="choose_best_model",
                        python_callable=_choose_best_model
                    )

                    task_5 = BashOperator(
                        task_id="accurate"
                        bash_command="echo 'accurate'"
                    )

                    task_6 = BashOperator(
                        task_id="inaccurate"
                        bash_command="echo 'inaccurate'"
                    )

                    [task_1, task_2, task_3] >> task_4 >> [task_5, task_6]

