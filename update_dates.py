import psycopg2
from psycopg2 import OperationalError
import json
import sys
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


def main():

    with open('correspondense_old_new.json') as f:
        datasets = json.load(f)
    try:
        connection = create_connection(config.db_name, config.db_user, config.db_password, config.db_host, config.db_port)
        for old_id in datasets:
            new_id = datasets[old_id]
            try:
                resp_old = config.api_origin.get_dataset_versions(old_id,True, False)
            except Exception as e:
                print("ERROR {}".format(e))
                continue

            if resp_old.status_code != 200:
                print("ERROR - ", resp_old.json())
                continue
            versions = resp_old.json()['data']
            versions.sort(key=take_version)
            try:
                resp_new = config.api_target.get_dataset_versions(new_id,True, False)
                if resp_new.status_code != 200:
                    print("ERROR - ",resp_new.json())
                    continue
            except Exception as e:
                print("ERROR {}".format(e))
                continue

            versions_new = resp_new.json()['data']
            versions_new.sort(key=take_version)
            print(versions_new)
            number_of_versions = len(versions)
            print(number_of_versions)
            n = 0
            if len(versions_new) == len(versions) or len(versions_new) != len(versions):
                for version in versions:
                    n = n + 1
                    last_update_time = ""
                    release_time = ""
                    versionState = ""
                    archiveNote = ""
                    archiveTime = ""
                    createTime = ""
                    query = ""
                    print(version)
                    if "lastUpdateTime" in version:
                        last_update_time = version['lastUpdateTime']
                        print(last_update_time)
                    if "releaseTime" in version:
                        release_time = version["releaseTime"]
                    if "versionState" in version:
                        versionState = version['versionState']
                    if "createTime" in version:
                        createTime = version['createTime']
                    if "archiveNote" in version:
                        archiveNote = version['archiveNote']
                    if "archiveTime" in version:
                        archiveTime = version['archiveTime']
                    try:
                        if versionState == "DRAFT":
                            if 'archiveTime' in version and 'archiveNote' in version:
                                query = "update datasetversion set lastupdatetime='{}'::timestamp AT time zone 'UTC', createtime ='{}'::timestamp AT time zone 'UTC', archivetime='{}'::timestamp AT time zone 'UTC', archivenote='{}' where dataset_id={} and versionState='DRAFT'".format(
                                    last_update_time, createTime, archiveTime, archiveNote, new_id)
                            else:
                                if 'archiveTime' in version:
                                    query = "update datasetversion set lastupdatetime='{}', createtime ='{}'::timestamp AT time zone 'UTC', archivetime='{}'::timestamp AT time zone 'UTC' where dataset_id={} and versionState='DRAFT'".format(
                                        last_update_time, createTime, archiveTime, new_id)
                                else:
                                    if 'archiveNote' in version:
                                        query = "update datasetversion set lastupdatetime='{}'::timestamp AT time zone 'UTC', createtime ='{}'::timestamp AT time zone 'UTC', archivenote='{}' where dataset_id={} and versionState='DRAFT'".format(
                                            last_update_time, createTime, archiveNote, new_id)
                                    else:
                                        query = "update datasetversion set lastupdatetime='{}'::timestamp AT time zone 'UTC', createtime ='{}'::timestamp AT time zone 'UTC' where dataset_id={} and versionState='DRAFT'".format(
                                            last_update_time, createTime, new_id)
                        if versionState == 'RELEASED':
                            version_number = version['versionNumber']
                            version_minor_number = version['versionMinorNumber']
                            if 'archiveTime' in version and 'archiveNote' in version:
                                query = "update datasetversion set lastupdatetime='{}'::timestamp AT time zone 'UTC', releasetime='{}'::timestamp AT time zone 'UTC', createtime='{}'::timestamp AT time zone 'UTC', archivetime='{}'::timestamp AT time zone 'UTC', archivenote='{}' where dataset_id={} and versionnumber={} and minorversionnumber={}".format(
                                    last_update_time, release_time, createTime, archiveTime, archiveNote, new_id,
                                    version_number, version_minor_number)
                            else:
                                if 'archiveTime' in version:
                                    query = "update datasetversion set lastupdatetime='{}'::timestamp AT time zone 'UTC', releasetime='{}'::timestamp AT time zone 'UTC', createtime='{}'::timestamp AT time zone 'UTC', archivetime='{}'::timestamp AT time zone 'UTC' where dataset_id={} and versionnumber={} and minorversionnumber={}".format(
                                        last_update_time, release_time, createTime, archiveTime, new_id,
                                        version_number, version_minor_number)
                                else:
                                    if 'archiveNote' in version:
                                        query = "update datasetversion set lastupdatetime='{}'::timestamp AT time zone 'UTC', releasetime='{}'::timestamp AT time zone 'UTC', createtime='{}'::timestamp AT time zone 'UTC', archivenote='{}' where dataset_id={} and versionnumber={} and minorversionnumber={}".format(
                                            last_update_time, release_time, createTime, archiveNote, new_id,
                                            version_number, version_minor_number)
                                    else:
                                        query = "update datasetversion set lastupdatetime='{}'::timestamp AT time zone 'UTC', releasetime='{}'::timestamp AT time zone 'UTC', createtime='{}'::timestamp AT time zone 'UTC' where dataset_id={} and versionnumber={} and minorversionnumber={}".format(
                                            last_update_time, release_time, createTime, new_id,
                                            version_number, version_minor_number)
                        print(query)
                        execute_query(connection, query)
                    except Exception as e:
                        print(e)
                        connection.close()
                        connection = create_connection(config.db_name, config.db_user, config.db_password,
                                                       config.db_host, config.db_port)



        connection.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
