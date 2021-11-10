import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(0, '/media/suyesh/01D7B522F84165F0/fuse/elasticSearch')

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from ESPhaseII import impData



with DAG("scheduler",
    start_date=datetime(2021, 1 ,1), 
    schedule_interval='@daily', 
    catchup=False) as dag:

    startElastic = BashOperator(
        task_id="startElastic",
        bash_command="sudo -S <<< \"233173\" -i service elasticsearch start"
    )

    stopElastic = BashOperator(
        task_id="stopElastic",
        bash_command="sudo -S <<< \"233173\" -i service elasticsearch stop"
    )
    
    uploadingDataToElastic = PythonOperator(
            task_id="uploadingDataToElastic",
            python_callable=impData
        ) 



    startElastic >> uploadingDataToElastic >> stopElastic