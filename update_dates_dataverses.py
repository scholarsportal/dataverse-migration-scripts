import json
import psycopg2
from psycopg2 import OperationalError
from config import Config

config = Config()

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The ERROR '{e}' occurred")
    return connection

def execute_query(connection, query):
    #connection.autocommit = True
    cursor = connection.cursor()
    print(cursor)
    try:
        cursor.execute(query)
        print("Query executed successfully")
        connection.commit()
    except OperationalError as e:
        print(f"The ERROR '{e}' occurred")

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The ERROR '{e}' occurred")

def main():

    with open('dataverse_dates.json') as fd:
        s = fd.read()
        s_new = s.replace("\\n", "")
        print(s_new)
        dates = json.loads(s_new)
        print(dates)

    connection = create_connection(config.db_name, config.db_user, config.db_password, config.db_host, config.db_port)

    for d in dates:
        try:
            id_old = d['id']
            resp = config.api_origin.get_dataverse(id_old)

            alias = resp.json()['data']['alias']
            resp = config.api_target.get_dataverse(alias)
            print(resp.json())
            new_id = resp.json()['data']['id']
            createdate = d['createdate']
            modificationtime = d['modificationtime']
            publicationdate = d['publicationdate']
            print(publicationdate)
            if publicationdate is None:
                query = "update dvobject set createdate='{}', modificationtime='{}' where id={} and dtype='Dataverse'".format(
                createdate, modificationtime, new_id)
            else:
                query = "update dvobject set publicationdate='{}', createdate='{}', modificationtime='{}' where id={} and dtype='Dataverse'".format(
                    publicationdate, createdate, modificationtime, new_id)
            print("query:" + query)
            execute_query(connection, query)
        except Exception as e:
            print(e)

    connection.close()
if __name__ == "__main__":
    main()
