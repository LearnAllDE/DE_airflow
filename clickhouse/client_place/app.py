#from clickhouse_driver import Client
import sys
import logging
from clickhouse_driver import Client

logging.warning("Log app started")



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
                   "artist Nullable(String),"
                   "song Nullable(String),"
                   "length Nullable(Float64)"
                   ")"
                   "ENGINE = Memory()"

                   )



if __name__ == '__main__':
    client = Client("ch")
    client.execute("DROP TABLE IF EXISTS blumb")
    startTable(client)
    logging.warning("Database is created")








