from clickhouse_driver import Client
import sys
import logging

logging.warning("Log app started")

client = Client('ch')
client.execute('SHOW TABLES')
client.execute('DROP TABLE IF EXISTS test')
client.execute('CREATE TABLE test (x Int32) ENGINE = Memory')
client.execute('INSERT INTO test (x) VALUES', [{'x': 100}])
client.execute('INSERT INTO test (x) VALUES', [[200]])
client.execute( 'INSERT INTO test (x) SELECT * FROM system.numbers LIMIT %(limit)s',{'limit': 3})


logging.warning(client.execute('SELECT sum(x) FROM test'))
logging.warning('All Good\n')

