import json
import psycopg2
from psycopg2 import OperationalError
from config import Config
import sys
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
        result = cursor.fetchone()[0]
        print("Query executed successfully")
        connection.commit()
        return result
    except OperationalError as e:
        print(f"The ERROR '{e}' occurred")

def execute_query_no_return(connection, query):
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


    with open('correspondence_old_new.json') as fd:
        datasets = json.load(fd)

    with open('dataverse_guestbook.json') as f:
        guestbookresponse = json.load(f)

    with open('dataverse_filedownloads.json') as f:
        filedownload = json.load(f)
    with open('users.json') as fu:
        users = json.load(fu)
    with open('all_data_files.json') as df:
        files = json.load(df)
    with open("dataverse_filemetadata.json") as m:
        filemetadatas = json.load(m)
    
    set_datasetversion = set()
    set_datafile = set()    
    versions = {}
    for fm in filemetadatas:
        if fm['datafile_id'] not in versions:
            a = []
            a.append( int(fm['datasetversion_id']))
            versions[fm['datafile_id']]= a
        else:
            a =  versions[fm['datafile_id']]
            a.append( int(fm['datasetversion_id']))
            versions[fm['datafile_id']]= a
     
   
    u = {}
    for user in users['data']['users']:
         u[str(user['id'])] = str(user['email'])

    missing_files = {}
    a = []
    b = set()
    c = set()
    all_response_ids = {}
    try:
        connection = create_connection(config.db_name, config.db_user, config.db_password, config.db_host, config.db_port)

        for guestbook in guestbookresponse:
            try:
                #email = "{0}".guestbook['email'] if guestbook['email'] == "NULL" else "NULL"
                #print( guestbook['dataset_id'])
                if guestbook['dataset_id'] in datasets:
                     dataset_id = str(datasets[guestbook['dataset_id']])
                else:
                    print(guestbook['dataset_id'])
                    continue
                email = "'" + guestbook['email'] + "'" if  guestbook['email'] != "NULL" else "NULL"
                institution =  "'" + guestbook['institution'] + "'" if  guestbook['institution'] != 'NULL' else "NULL"
                name =  "'" + guestbook['name'] + "'" if  guestbook['name'] != 'NULL' else "NULL"
                position =  "'" + guestbook['position'] + "'" if  guestbook['position'] != 'NULL' else "NULL"
                responsetime =  "'" + guestbook['responsetime'] + "'" if  guestbook['responsetime'] != 'NULL' else "NULL"
                if guestbook['authenticateduser_id'] != "NULL":
                    query3 = "select id from authenticateduser where email ilike" + "'" + u[ guestbook['authenticateduser_id']] + "'"
                    print(query3)
                    authenticateduser_id = execute_read_query(connection, query3)
                    authenticateduser_id = str(authenticateduser_id[0][0])
                else:
                    authenticateduser_id = "NULL"

                #authenticateduser_id = str(u[guestbook['email']]) if  guestbook['email'] in u else "NULL" 
                if  guestbook['datafile_id'] not in files:
                    mf = {}
                    mf["dataset_id"] = dataset_id
                    mf["datafile_id"] = guestbook['datafile_id']
                    b.add( guestbook['datafile_id'])
                    c.add( guestbook['dataset_id'])
                    a.append(mf)

                    #print( guestbook['datafile_id'])
                datafile_id = str(files[guestbook['datafile_id']]) if guestbook['datafile_id'] in files else "NULL"  
                new_datasetversion_id = "NULL"
                if guestbook['datasetversion_id'] != "NULL" and datafile_id != "NULL":
                    query2 = "select datasetversion_id from filemetadata where datafile_id =" + datafile_id + ";"
                    datasetversion_id = execute_read_query(connection, query2)
                    if len(datasetversion_id) == 1:
                        new_datasetversion_id = datasetversion_id[0][0]
                    else:
                        vs = []
                        for v in datasetversion_id:
                            vs.append(v[0])
                        vs.sort()
                        vs_org = versions[guestbook['datafile_id']]
                        vs_org.sort()
                        i = 0
                        for v in vs_org:
                            if v == int(guestbook['datasetversion_id']):
                               new_datasetversion_id = vs[i]
                               break
                            else:
                               i = i+1
        
                if (datafile_id != "NULL"):        
                  query = "insert into guestbookresponse" \
                        "(email,institution,name,position,responsetime,dataset_id,authenticateduser_id,datafile_id,datasetversion_id,guestbook_id) values " \
                        "({0},{1},{2},{3},{4},{5},{6},{7},{8},{9}) " \
                        " returning id;".format(email, institution, name,
                                                position, responsetime,dataset_id,authenticateduser_id,datafile_id,new_datasetversion_id,1)

                  print(query)      
                  my_id = execute_query(connection, query)
                
                  all_response_ids[str(guestbook["id"])] = my_id  
                  #if guestbook['datasetversion_id'] != "NULL":
            except Exception as e:
                print(e)
                connection.close()
                break
                connection = create_connection(config.db_name, config.db_user, config.db_password,
                                                           config.db_host, config.db_port)
        
        insert_filedownload(all_response_ids,filedownload,connection)
        connection.close()
        missing_files["files"]=a
    except Exception as e:
        print(e)

def  insert_filedownload(all_response_ids,filedownloads, connection):
    print("Start  insert_filedownload")
    print( all_response_ids)
    for f  in filedownloads:
       if f["guestbookresponse_id"] in all_response_ids:  
         my_id = all_response_ids[f["guestbookresponse_id"]]
         print(my_id)
         print("Hi")
         query = "insert into filedownload(downloadtimestamp,downloadtype,guestbookresponse_id,sessionid) values ('{0}','{1}',{2},'{3}')".format(f["downloadtimestamp"],
                                                                                                                                          f["downloadtype"],
                                                                                                                                          str(my_id),
                                                                                                                                          f["sessionid"])
         print(query)
         try:
                execute_query_no_return(connection, query)
         except Exception as e:
                print(e)
                connection.close()
                break
                connection = create_connection(config.db_name, config.db_user, config.db_password,
                                                           config.db_host, config.db_port)



        
if __name__ == "__main__":
    main()
