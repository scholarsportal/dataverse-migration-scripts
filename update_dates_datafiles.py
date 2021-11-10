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


    with open('datafile_dates.json') as fd:
        s = fd.read()
        s_new = s.replace("\\n", "")
        dates = json.loads(s_new)

    with open('all_data_files.json') as f:
        corr = json.load(f)

    connection = create_connection(config.db_name, config.db_user, config.db_password, config.db_host, config.db_port)

    for d in dates:
        try:
            id_old = d['id']

            dt = {}
            dt['createdate'] = d['createdate']
            dt['modificationtime'] = d['modificationtime']
            dt['publicationdate'] = d['publicationdate']
            new_id = corr[str(id_old)]



            createdate = d['createdate']
            modificationtime = d['modificationtime']
            publicationdate = d['publicationdate']
            r=execute_read_query(connection, "select * from dvobject where id={}".format(new_id))
            print(len(r))
            for a in r:
                print(r)
            if publicationdate is None:
                query = "update dvobject set createdate='{}', modificationtime='{}' where id={} and dtype='DataFile'".format(
                createdate, modificationtime, new_id)
            else:
                query = "update dvobject set publicationdate='{}', createdate='{}', modificationtime='{}' where id={} and dtype='DataFile'".format(
                    publicationdate, createdate, modificationtime, new_id)

            execute_query(connection, query)
            print(query)
        except Exception as e:
            print("Error {}".format(e))

    connection.close()
if __name__ == "__main__":
    main()