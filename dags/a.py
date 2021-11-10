
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from random import randint
import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, '/media/suyesh/01D7B522F84165F0/fuse/elasticSearch')


from ESPhaseII import impData


def bla():
    pass

with DAG("test",
    start_date=datetime(2021, 1 ,1), 
    schedule_interval='@daily', 
    catchup=False) as dag:



    uploading_data_to_elastic =PythonOperator(
        task_id="uploading_data_to_elastic",
        python_callable=impData
    )
    
    start_elastic = BashOperator(
        task_id="start_elastic",
        bash_command="sudo -S <<< \"233173\" -i service elasticsearch start"
    )

    stop_elastic = BashOperator(
        task_id="stop_elastic",
        bash_command=" sudo -S <<< \"233173\" -i service elasticsearch stop"
    )

    start_elastic >> uploading_data_to_elastic >>stop_elastic
