# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import json
import os
from clickhouse_driver import Client
import sys
import logging




def json_iter(json_file_path):
    with open(json_file_path) as reader:
        for line in reader:
            line = json.loads(line)
            list_check = {"ts":0,"userId":"","sessionId":0,"page":"","auth":"","method":"","status":0,"level":"","itemInSession":0,"location":"","userAgent":"","lastName":"","firstName":"","registration":0,"gender":"","artist":"","song":"","length":0.0}
            lack = list(set(line.keys())^set(list_check.keys()))
            for i in lack:
                line[i] = list_check[i]
            yield line

def startTable(client):


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
                   "artist String DEFAULT toString('no'),"
                   "song String DEFAULT toString('no'),"
                   "length Float64 DEFAULT toFloat64(0)"
                   ")"
                   "ENGINE = Log()"
                   )



# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    print(os.environ)
    client = Client('localhost')

    if (client.execute("EXISTS TABLE blumb")[0][0] == 1):
        print(1)

    client.execute('DROP TABLE IF EXISTS blumb')
    startTable(client)
    print("Write json")
    json_file = '/home/aquafeet/dep/event-data.json'

    sets = {"input_format_null_as_default":1,"input_format_values_interpret_expressions" :1}
    client.execute("INSERT INTO blumb VALUES", (line for line in json_iter(json_file)), with_column_types=True, settings=sets, types_check=False)
    print("Enter")
    a = (client.execute("SELECT * FROM blumb",columnar = True,with_column_types=True))
    print("Lines inside table")
    print(client.execute("SELECT COUNT(song), count(artist) FROM blumb GROUP BY song", columnar = True))
   # print(client.execute(" SELECT gender, lastName  FROM blumb WHERE gender = 'M' "))

    '''
    client.execute('SHOW TABLES')
    client.execute('DROP TABLE IF EXISTS test')
    client.execute('CREATE TABLE test (x Int32) ENGINE = Memory')
    client.execute('INSERT INTO test (x) VALUES', [{'x': 100}])
    client.execute('INSERT INTO test (x) VALUES', [[200]])
    client.execute('INSERT INTO test (x) SELECT * FROM system.numbers LIMIT %(limit)s', {'limit': 3})

    logging.warning(client.execute('SELECT sum(x) FROM test'))
    logging.warning('All Good\n')

    a = (client.execute("SELECT * FROM test", columnar=True, with_column_types=True))

    print(a)

    print(client.execute("DESC TABLE test", columnar = False))
'''



