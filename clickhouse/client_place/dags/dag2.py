from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta
import os

def my_func(**op_args):
    print(op_args)
    destdir = '/opt/data'
    files = [ f for f in os.listdir(destdir) if os.path.isfile(os.path.join(destdir,f)) ]
    print(files)
    return op_args['param1']

defaults = {
	'owner':'sashka',
	'depends_on_past': False,
	'start_date':datetime(2020,8,8),
	'retries':1,
	'retry_delay':timedelta(minutes=1)
} 

dag = DAG('try1', default_args = defaults,schedule_interval = "* * * * *")


#dummy_task = DummyOperator(task_id='dummy_task', dag = dag)
t1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Hello World from Task 1"',
    dag=dag)

dum = DummyOperator(task_id = "d", dag = dag)
python_task= PythonOperator(task_id='python_task',provide_context = True, python_callable=my_func, op_kwargs = {'param1':'one','param2':'two'}, dag=dag)


t1 >> python_task >> dum
t1 >> dum


