from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from clickhouse_driver import Client
from datetime import datetime, timedelta
import logging
import json
import os






def table_create(**op_args):
    client = Client("ch")
    #client.execute("DROP TABLE IF EXISTS blumb")
    print("ENV")
    print(os.environ)
    #logging.warning(os.environ['LOOK'])
    if(client.execute("EXISTS TABLE blumb")[0][0] == 1):
        pass
    else:
    #startTable(client)
        client.execute("CREATE TABLE blumb ("
                   "ts Int64,"
                   "userId String,"
                   "sessionId Int64,"
                   "page String,"
                   "auth String,"
                   "method String,"
                   "status Int64,"
                   "level String,"
                   "itemInSession Int64,"
                   "location String,"
                   "userAgent String,"
                   "lastName String,"
                   "firstName String,"
                   "registration Int64,"
                   "gender String,"
                   "artist String,"
                   "song String,"
                   "length Float64"
                   ")"
                   "ENGINE = Log()"
                   )
    logging.warning("Database is created")


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

dag = DAG('run1', default_args = defaults,schedule_interval = os.environ["SH"],catchup=False)

def json_iter(json_file_path):
    with open(json_file_path) as reader:
        for line in reader:
            line = json.loads(line)
            list_check = {"ts":0,"userId":"","sessionId":0,"page":"","auth":"","method":"","status":0,"level":"","itemInSession":0,"location":"","userAgent":"","lastName":"","firstName":"","registration":0,"gender":"","artist":"","song":"","length":0.0}
            lack = list(set(line.keys())^set(list_check.keys()))
            for i in lack:
                line[i] = list_check[i]
            yield line



def send_data(**op_args):
    json_file = "/opt/data/event-data.json"
    client = Client("ch")
    sets = {"input_format_null_as_default":1,"input_format_values_interpret_expressions" :1, "input_format_skip_unknown_fields" :1}
    client.execute("INSERT INTO blumb FORMAT JSONEachRow", (line for line in json_iter(json_file)), with_column_types=True, settings=sets, types_check=False)
    

def show(**op_args):
    client = Client("ch")
    a = (client.execute("SELECT COUNT(*) FROM blumb",columnar = False,with_column_types=False))
    print("Lines inside table")
    print(a)
#dummy_task = DummyOperator(task_id='dummy_task', dag = dag)
#t1 = BashOperator(
 #   task_id='task_1',
  #  bash_command='echo "Hello World from Task 1"',
   # dag=dag)



#dum = DummyOperator(task_id = "d", dag = dag)
create_data = PythonOperator(task_id = 'create_data', provide_context = True, python_callable = table_create, dag = dag)
send_data= PythonOperator(task_id='send_data',provide_context = True, python_callable=send_data, dag=dag)
analytic = PythonOperator(task_id = "analytics", provide_context = True, python_callable = show ,dag = dag)

create_data >> send_data >> analytic


