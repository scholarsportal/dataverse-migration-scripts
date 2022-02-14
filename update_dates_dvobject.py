import psycopg2
from psycopg2 import OperationalError
import json
import sys
from config import Config

config = Config()

def take_version(elem):
    if 'versionNumber' not in elem:
        return sys.maxsize

    v = elem['versionNumber']
    if v == ':draft':
        return sys.maxsize
    else:
        mv = elem['versionMinorNumber']
        if mv == 0:
            return v
        else:
            return v + mv / (len(str(mv)) * 10)

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
        connection.rollback()

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The ERROR '{e}' occurred")

def take_version(elem):
    print("Take version")
    if 'versionNumber' not in elem:
        return sys.maxsize

    v = elem['versionNumber']
    if v == ':draft':
        return sys.maxsize
    else:
        mv = elem['versionMinorNumber']
        print(mv)
        if mv == 0:
            print(v)
            return v
        else:
            print(v + mv / (len(str(mv)) * 10))
            return v + mv / (len(str(mv)) * 10)

def main():

    with open('correspondence_old_new.json') as f:
        datasets = json.load(f)
    try:
        connection = create_connection(config.db_name, config.db_user, config.db_password, config.db_host, config.db_port)
        for old_id in datasets:
            try:
                new_id = datasets[old_id]

                resp_ver_origin = config.api_origin.get_dataset_versions(old_id, True, False)
                if resp_ver_origin.status_code != 200:
                    print("ERROR - ", resp_ver_origin.json())
                    sys.exit()
                else:
                    print("Alberta")
                    print(resp_ver_origin.json()['data'])

                resp_ver_target = config.api_target.get_dataset_versions(new_id, True, False)
                if resp_ver_target.status_code != 200:
                    print("ERROR - ", resp_ver_origin.json())
                    sys.exit()
                else:
                    print("Alberta")
                    print(resp_ver_target.json()['data'])

                resp = config.api_origin.get_dataset(old_id, ":latest", True, False)

                if resp.status_code != 200:
                    print("ERROR - ", resp.json())
                    sys.exit()
                else:
                    print("Alberta")
                    print(resp.json()['data'])

                latest_version = resp.json()['data']['latestVersion']

                all_versions_origin = resp_ver_origin.json()['data']
                all_versions_origin.sort(key=take_version)
                all_versions_target = resp_ver_target.json()['data']
                all_versions_origin.sort(key=take_version)
                n = len(all_versions_origin) - len(all_versions_target)
                print(n)

                if (n == 0 or n != 0):
                    first_version = all_versions_origin[n]
                    print("First version")
                    print(first_version)
                    createdate = ''
                    modificationtime = ''
                    if "releaseTime" in first_version:
                        publicationdate = first_version['releaseTime']
                        print(publicationdate)
                        if 'createTime' in first_version and 'lastUpdateTime' in latest_version:
                            createdate = first_version['createTime']
                            modificationtime = latest_version['lastUpdateTime']
                            query = "update dvobject set publicationdate='{}', createdate='{}'::timestamp AT time zone 'UTC', modificationtime='{}'::timestamp AT time zone 'UTC' where id={} and dtype='Dataset'".format(
                            publicationdate, createdate, modificationtime, new_id)
                            print(query)
                            execute_query(connection, query)
                        elif 'createTime' in first_version:
                            createdate = first_version['createTime']
                            query = "update dvobject set publicationdate='{}', createdate='{}'::timestamp AT time zone 'UTC' where id={} and dtype='Dataset'".format(
                                publicationdate, createdate, new_id)
                            print(query)
                            execute_query(connection, query)
                        elif 'lastUpdateTime' in latest_version:
                            modificationtime = latest_version['lastUpdateTime']
                            query = "update dvobject set publicationdate='{}', modificationtime='{}'::timestamp AT time zone 'UTC' where id={} and dtype='Dataset'".format(
                                publicationdate, modificationtime, new_id)
                            print(query)
                            execute_query(connection, query)
                    else:
                        if 'lastUpdateTime' in latest_version and 'createTime' in first_version:
                            modificationtime = latest_version['lastUpdateTime']
                            createdate = first_version['createTime']
                            query = "update dvobject set createdate='{}'::timestamp AT time zone 'UTC', modificationtime='{}'::timestamp AT time zone 'UTC' where id={} and dtype='Dataset'".format(
                            createdate, modificationtime, new_id)
                            print(query)
                            execute_query(connection, query)
                        elif 'createTime' in first_version:
                            createdate = first_version['createTime']
                            query = "update dvobject set createdate='{}'::timestamp AT time zone 'UTC' where id={} and dtype='Dataset'".format(
                                createdate, new_id)
                            print(query)
                            execute_query(connection, query)
                        elif 'lastUpdateTime' in latest_version:
                            modificationtime = latest_version['lastUpdateTime']
                            query = "update dvobject set modificationtime='{}'::timestamp AT time zone 'UTC' where id={} and dtype='Dataset'".format(
                                modificationtime, new_id)
                            print(query)
                            execute_query(connection, query)
                else:
                    print("WARNING: different number of versions")

            except Exception as e:
                print("Error {}".format(e))
                continue

        connection.close()
    except Exception as e:
        print("Error {}".format(e))

if __name__ == "__main__":
    main()
