from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _world():
    print('a')

with DAG(
    dag_id='my_second_dag',
    start_date=timezone.datetime(2024, 1,22),
    #schedule=None,
    schedule='0 0 * * *',
): 
    start=EmptyOperator(task_id='start')
    
    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello'",
    )

   
    world=PythonOperator(
    task_id='world',
    python_callable=_world,
    )

    end=EmptyOperator(task_id='end')

    #start >> hello >> end
    #start >> world >> end
    start >> [hello,world] >> end
