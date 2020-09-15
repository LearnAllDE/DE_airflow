# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import json
import os
from clickhouse_driver import Client
import sys
import logging

def startTable(client):
    json_data = []
    json_file = '/home/aquafeet/dep/event-data.json'
    file = open(json_file)
    #with open(json_file, 'r') as file:
     #   data = file.read().replace('\n', ' ')
    #print(data)'''

    for line in file:
        json_line = json.loads(line)
        json_data.append(json_line)



    client.execute("CREATE TABLE blumb ("
                   "ts Int64,"
                   "userId String,"
                   "sessionId Int64,"
                   "page String,"
                   "auth String,"
                   "method String,"
                   "status Int8,"
                   "level String,"
                   "itemInSession Int8,"
                   "location String,"
                   "userAgent String,"
                   "lastName String,"
                   "firstName String,"
                   "registration Int64,"
                   "gender String,"
                   "artist String,"
                   "song String,"
                   "length Float32"
                   ")"
                   "ENGINE = Log()"
                   
                   "SETTINGS index_granularity = 8192")
    return json_data


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    client = Client('localhost')

    json_data = 0

    client.execute('DROP TABLE IF EXISTS blumb')
    json_data = startTable(client)


    print(json_data[0:5])
    print("Write json")
    print(client.execute("SELECT * FROM system.settings WHERE name IN('max_execution_time', 'receive_timeout')"))


    sets = {'input_format_skip_unknown_fields': 1, 'max_insert_block_size':100}
    client.execute_with_progress("INSERT INTO blumb FORMAT JSON", json_data[0:5], with_column_types=False, settings=sets,
                   types_check=False)



    print(client.execute("SELECT COUNT(*) FROM blumb"))
    print("Lines inside table")
    print(client.execute(" SELECT gender, lastName  FROM blumb WHERE gender = 'M' "))


    '''client.execute('SHOW TABLES')
    client.execute('DROP TABLE IF EXISTS test')
    client.execute('CREATE TABLE test (x Int32) ENGINE = Memory')
    client.execute('INSERT INTO test (x) VALUES', [{'x': 100}])
    client.execute('INSERT INTO test (x) VALUES', [[200]])
    client.execute('INSERT INTO test (x) SELECT * FROM system.numbers LIMIT %(limit)s', {'limit': 3})

    logging.warning(client.execute('SELECT sum(x) FROM test'))
    logging.warning('All Good\n')'''





