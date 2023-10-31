from config import Config
from pyDataverse.models import Datafile
import json
import sys
import requests
import io
import time
from datetime import datetime
import logging
import zipfile

config = Config()
total_files_names = json.loads('{}')

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

def check_lock(DOI_NEW):
    try:
        lock = config.api_target.get_dataset_lock(DOI_NEW)
        if lock.status_code == 503:
            logging.critical("503 - Server {} is unavailable".format(config.base_url_target))
            sys.exit()
        a = 0
        while len(lock.json()['data']) > 0 and a < 300:
            logging.debug("Lock {} times {} {}".format(str(a), DOI_NEW, lock.json()))
            time.sleep(10)
            a = a + 1
            lock = config.api_target.get_dataset_lock(DOI_NEW)
            if lock.status_code == 503:
                logging.critical("503 - Server {} is unavailable".format(config.base_url_target))
                sys.exit()
            if lock.status_code != 200:
                logging.error("check_lock func: lock status {} for {}".format(lock.status_code, DOI_NEW))
                return False
    except Exception as e:
        logging.error("check_lock. Error {}, dataset {} ".format(e, DOI_NEW))
        return False
    return True

def find_extension(originalFileFormat):
    original = originalFileFormat.lower()
    if original == "application/x-spss-sav":
        return ".sav"
    elif original == "application/x-spss-por":
        return ".por"
    elif original == "application/x-stata":
        return ".dta"
    elif original == "application/x-dvn-csvspss-zip":
        return ".zip"
    elif original == "application/x-dvn-tabddi-zip":
        return ".zip"
    elif original == "application/x-rlang-transport":
        return ".RData"
    elif original == "text/csv":
        return ".csv"
    elif original == "text/comma-separated-values":
        return ".csv"
    elif original == "text/tsv":
        return ".tsv"
    elif original == "text/tab-separated-values":
        return ".tsv"
    elif original == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        return ".xlsx"
    return ""

def publish_version(vm, DOI_NEW):
    check = check_lock(DOI_NEW)
    if check == False:
        return check
    try:
        if vm != 0:
            resp = config.api_target.publish_dataset(DOI_NEW,'minor')
        else:
            resp = config.api_target.publish_dataset(DOI_NEW, 'major')
        if resp.status_code == 200:
            return True
        else:
            logging.error("publish_version func: Error publishing dataset {} status {}".format(DOI_NEW, resp.status_code))
            return False
    except Exception as e:
        logging.error("publish_version error. Error {}, dataset {} ".format(e, DOI_NEW))
        return False

    return True


def update_file_metadata(prev_file, file, field):
    if (field in file and field not in prev_file) or (field not in file and field in prev_file):
        return True
    else:
        if field in file and field in prev_file:
            if file[field] != prev_file[field]:
                return True

    return False

def check_add_files(files_list, total_files):
    add_files_list = []
    updated_files_list = []

    for file in files_list:
        old_id = file["dataFile"]["id"]
        if old_id not in total_files:
            add_files_list.append(file)
        else:
            prev_file = total_files[old_id]['file']
            update= update_file_metadata(prev_file, file, "description")
            if update:
                updated_files_list.append(file)
            else:
                update = update_file_metadata(prev_file, file, "label")
                if update:
                    updated_files_list.append(file)
                else:
                    update = update_file_metadata(prev_file, file, "directoryLabel")
                    if update:
                        updated_files_list.append(file)
                    else:
                        update = update_file_metadata(prev_file, file, "categories")
                        if update:
                            updated_files_list.append(file)
                        else:
                            update = update_file_metadata(prev_file, file, "restricted")
                            if update:
                                updated_files_list.append(file)
                            else:
                                update = update_file_metadata(prev_file, file, "provFreeform")
                                if update:
                                    updated_files_list.append(file)

    return add_files_list, updated_files_list

def check_delete_files(files_list, total_files):
    delete_files_list = []
    for id_old in total_files:
        found = False
        for file in files_list:
            id = file["dataFile"]["id"]
            if (id == id_old):
                found = True
                break
        if not found:
            delete_files_list.append(total_files[id_old]['file'])
    return delete_files_list

def delete_files(files_list, total_files, DOI_NEW):
    
    for file in files_list:
        old_id = file['dataFile']['id']
        new_id = total_files[old_id]['new_id']
        url = config.base_url_target + "/dvn/api/data-deposit/v1.1/swordv2/edit-media/file/" + str(new_id)
        check = check_lock(DOI_NEW)
        if check == False:
            return check
        req = requests.delete(url, auth=(config.api_token_target, ''))
        if req.status_code == 503:
            logging.critical("503 - Server is unavailable {}", config.base_url_target)
            sys.exit()
        if req.status_code == 200 or req.status_code == 204:
            del total_files[old_id]
        else:
            logging.error("delete_files func: Error to delete file in {}, with {}, status {}".format(DOI_NEW, url, req.status_code))
            return False
    return True

def update_files(files_list, total_files, DOI_NEW):
    headers = {'X-Dataverse-key': config.api_token_target}
    for file in files_list:
        old_id = file["dataFile"]["id"]
        new_id = total_files[old_id]['new_id']
        metadata_json = {}
        if 'description' in file:
            metadata_json["description"] = file['description']
        #if 'label' in file:
        #    metadata_json["label"] = file['label']
        if 'directoryLabel' in file:
            metadata_json["directoryLabel"] = file['directoryLabel']
        if 'categories' in file:
            metadata_json["categories"] = file['categories']
        if 'restricted' in file:
            metadata_json["restrict"] = file['restricted']
        if 'provFreeform' in file:
            metadata_json["provFreeform"] = file['provFreeform']

        url = config.base_url_target + '/api/files/{0}/metadata'.format(new_id)

        json_input = io.StringIO(json.dumps(metadata_json))
        files = { 'jsonData': json_input}
        check = check_lock(DOI_NEW)
        if check == False:
            return check
        resp = requests.post(url, files=files, headers=headers)
        logging.debug("Update file metadata status ".format(resp.status_code))
        if resp.status_code == 503: 
            logging.critical("503 - Server is unavailable {}", config.base_url_target)
            sys.exit()


def add_files(files_list, DOI_NEW, dir_path, total_files):
    return(True)

    for file in files_list:
        logging.info("-----------------------");
        logging.info("Starting {0}".format(file["dataFile"]["filename"]))
        try:
            filename_zip = False
            filename = file["dataFile"]["filename"]
            index = file["dataFile"]['storageIdentifier'].rfind('/')
            storageIdentifier = file["dataFile"]['storageIdentifier'][index+1:]
            if 'originalFileFormat' in file["dataFile"]:
                originalFileFormat = file["dataFile"]['originalFileFormat']
                logging.info("Original file format {0}".format(originalFileFormat))
                ext = find_extension(originalFileFormat)
                index = filename.rfind(".")
                filename = filename[0:index] + ext
                typeFile = originalFileFormat
                storageIdentifier = storageIdentifier + ".orig"

            else:
                typeFile = file["dataFile"]["contentType"]
                if typeFile == "application/zip":
                    filename_zip = True
                    f_output = dir_path + '/' + filename
                    f_input = dir_path + '/' +storageIdentifier
                    with zipfile.ZipFile(f_output, 'w') as myzip:
                        myzip.write(f_input, filename)

            df = Datafile()
            logging.info("Type file {0}".format(typeFile))
            df_dict = {
                "pid": DOI_NEW,
                "filename": filename,
                'contentType': typeFile
            }
            if 'categories' in file:
                df_dict['categories'] = file['categories']
            if 'persistentId' in file["dataFile"]:
                df_dict['persistentId'] = file["dataFile"]['persistentId']
            if 'pidURL' in file["dataFile"]:
                df_dict['pidURL'] = file["dataFile"]['pidURL']
            if 'description' in file:
                df_dict['description'] = file['description']
            if 'restricted' in file:
                df_dict['restrict'] = file['restricted']
            if 'directoryLabel' in file:
                df_dict['directoryLabel'] = file['directoryLabel']
            #if 'label' in file:
            #    df_dict['label'] = file['label']
            if 'provFreeform' in file:
                df_dict["provFreeform"] = file['provFreeform']


            df.set(df_dict)
            logging.info(df.json());
            logging.info("It is filename {0}".format(filename))
            if not filename_zip:
                full_filename = dir_path + '/'  + storageIdentifier
            else:
                full_filename = dir_path + '/' + filename
            url = config.api_target.base_url_api_native
            url += "/datasets/:persistentId/add?persistentId={0}".format(DOI_NEW)
            files = {'file': (filename, open(full_filename, 'rb'), typeFile)}
            headers = {'X-Dataverse-key': config.api_token_target}
            check = check_lock(DOI_NEW)
            if check == False:
                return check
            resp = requests.post(url, data={"jsonData": df.json()}, files=files, headers=headers)

            if resp.status_code == 503:
                logging.critical("503 - Server {} is unavailable".format(config.base_url_target))
                sys.exit()
            if resp.status_code == 200:
                old_id = file["dataFile"]["id"]

                total_files[old_id] = {
                    'new_id': resp.json()['data']['files'][0]['dataFile']['id'],
                    'file': file
                }
            else:
                logging.error("add_files func: Error adding file. Error {}, filename {}, storageIdentifier {}, directory {}".format(resp.status_code, filename, storageIdentifier, dir_path) )
                return False

        except Exception as e:
            logging.error("add_files error. Error {}, filename {}, storageIdentifier {}, directory {}".format(e, filename, storageIdentifier, dir_path))
            return False

    return True
def clean_version(version):
    if 'id' in version:
        del version['id']
    if 'datasetId' in version:
        del version['datasetId']
    if 'datasetPersistentId' in version:
        del version['datasetPersistentId']
    if 'storageIdentifier' in version:
        del version['storageIdentifier']
    if 'versionNumber' in version:
        del version['versionNumber']
    if 'versionMinorNumber' in version:
        del version['versionMinorNumber']
    if 'versionState' in version:
        del version['versionState']
    if 'UNF' in version:
        del version['UNF']
    if 'versionNote' in version:
        del version['versionNote']
    if 'releaseTime' in version:
        del version['releaseTime']
    if 'lastUpdateTime' in version:
            del version['lastUpdateTime']

    if 'files' in version:
        del version['files']

    #for 5.4.1 (Producer is mandatory)
    # producer = False
    # if 'metadataBlocks' in version:
    #     if 'citation' in version['metadataBlocks']:
    #         if 'fields' in version['metadataBlocks']['citation']:
    #             fields = version['metadataBlocks']['citation']['fields']
    #             for field in fields:
    #                 if field['typeName'] == 'producer':
    #                     producer = True
    #                     break
    # if not producer:
    #     fieldSubject = {
    #         "typeName": "producer",
    #         "multiple": True,
    #         "typeClass": "compound",
    #         "value": [
    #             {
    #                 "producerName": {
    #                     "typeName": "producerName",
    #                     "multiple": False,
    #                     "typeClass": "primitive",
    #                     "value": "UAL"
    #                 }
    #             }
    #         ]
    #     }
    #     if 'metadataBlocks' in version:
    #         if 'citation' in version['metadataBlocks']:
    #             if 'fields' in version['metadataBlocks']['citation']:
    #                 version['metadataBlocks']['citation']['fields'].append(fieldSubject)


    return version

def check_and_update_subject(version):
    subject = False
    if 'metadataBlocks' in version:
        if 'citation' in version['metadataBlocks']:
            if 'fields' in version['metadataBlocks']['citation']:
                fields = version['metadataBlocks']['citation']['fields']
                for field in fields:
                    if field['typeName'] == 'subject':
                        subject = True
                        break
    if not subject:
        fieldSubject = {
            "typeName": "subject",
            "multiple": True,
            "typeClass": "controlledVocabulary",
            "value": [
                "Other"
            ]
        }
        if 'metadataBlocks' in version:
            if 'citation' in version['metadataBlocks']:
                if 'fields' in  version['metadataBlocks']['citation']:
                    version['metadataBlocks']['citation']['fields'].append(fieldSubject)

    return version

def create_version(version, only, DOI, DOI_NEW, dir_path, total_files, correspondence, first):

    try:
        if 'versionState' in version:
            if version['versionState'] == 'DEACCESSIONED':
                return True, correspondence, first
            if version['versionState'] == 'DRAFT':
                v = ':draft'
                vm = 0
            else:
                if 'versionNumber' in version:
                    v = version['versionNumber']
                    vm = version['versionMinorNumber']
        else:
            if 'versionNumber' in version:
                v = version['versionNumber']
                vm = version['versionMinorNumber']

        old_dataset_id = version['datasetId']
        if 'files' in version:
            files_list = version['files']
        version = clean_version(version)

        if first:
            s = json.dumps(version)
            s = "{ \"datasetVersion\":" + s + "}"

            if (v != ':draft'):
                resp = config.api_target.create_dataset(config.dataverse_alias, s, DOI, False)
            else:
                resp = config.api_target.create_dataset(config.dataverse_alias, s)
                if resp.status_code == 201:
                    DOI_NEW = resp.json()['data']['persistentId']
                else:
                    logging.error(resp.json())
            if resp.status_code == 503:
                logging.critical("503 - Server {} is unavailable".format(config.base_url_target))
                sys.exit()
            if resp.status_code == 200 or resp.status_code == 201:
                first = False
                new_dataset_id = resp.json()['data']['id']
                correspondence[old_dataset_id] = new_dataset_id # old dataset id with new one
                status = add_files(files_list,  DOI_NEW, dir_path, total_files)
                if status == False:
                    return False, correspondence, first
                if v != ':draft':
                    status = publish_version(vm, DOI_NEW)
                    if status == False:
                        return False, correspondence, first
            else:
                logging.error("create_version func: Create dataset error for {}, status {}".format( DOI, resp.status_code))
                return False, correspondence, first
        else:
            # Update metadata
            version = check_and_update_subject(version)
            url = config.api_target.base_url_api_native
            url += "/datasets/:persistentId/versions/:draft?persistentId={0}".format(DOI_NEW)
            check_lock(DOI_NEW)
            r = config.api_target.put_request(url, data=json.dumps(version), auth=True)
            if r.status_code == 503:
                logging.critical("503 - Server {} is unavailable".format(config.base_url_target))
                sys.exit()
            if r.status_code != 200:
                logging.error("create_version func: Update dataset metadata error for {}, status {}".format(DOI, r.status_code) )
                return False, correspondence, first

            # Delete files
            du_files = check_delete_files(files_list, total_files)
            if len(du_files) > 0:
                status = delete_files(du_files, total_files, DOI_NEW)
                if status == False:
                    return False, correspondence, first
                r = config.api_target.get_dataset(DOI_NEW, ":latest")
                if r.status_code == 503:
                    logging.critical("503 - Server {} is unavailable".format(config.base_url_target))
                    sys.exit()
                if r.status_code != 200:
                    logging.error("create_version func: Get dataset metadata error for {}, status {}".format(DOI, r.status_code ))
                    return False, correspondence, first
                fields = r.json()['data']['latestVersion']['metadataBlocks']['citation']['fields']
                for field in fields:
                    if field['typeName'] == 'producer':
                        fs = version['metadataBlocks']['citation']['fields']
                        ex = False
                        for f in fs:
                            if field['typeName'] == 'producer':
                                ex = True
                                break
                        if not ex:
                            version['metadataBlocks']['citation']['fields'].append(field)
                        break

            # Add files
            a_files = check_add_files(files_list, total_files)
            status = add_files(a_files[0], DOI_NEW, dir_path, total_files)
            if status == False:
                return False, correspondence, first
            update_files(a_files[1], total_files, DOI_NEW)
            if v != ':draft':
                status = publish_version(vm, DOI_NEW)
                if status == False:
                    return False, correspondence, first

    except Exception as e:
        logging.error("create_version func: dataset {} Error {}".format(DOI,e))
        return False, correspondence, first

    return True, correspondence, first

def get_datasets(parent, datasets):
    print(parent)
    resp = config.api_origin.get_dataverse_contents(parent)
    if resp.status_code == 200:
        dataverses = resp.json()['data']
        if len(dataverses) != 0:
            for elem in dataverses:
                if elem['type'] == 'dataverse':
                    datasets = get_datasets(elem['id'], datasets)

                if elem['type'] == 'dataset':
                    datasets.append(elem['identifier'])

    return  datasets

def main():
    logging.basicConfig(filename='versions_files.log', level=logging.INFO)
    now = datetime.now()
    logging.info("Program started {}".format(now))
    logging.info(config.dr)
    logging.info("Number of datasets {}".format(len(config.directories)))
    correspondence = {}
    all_total_files = {}
    for directory in config.directories:
        try:
            now = datetime.now()
            logging.info("Dataset {} started {} ".format(directory, now))
            total_files = json.loads('{}')
            DOI = "doi:" + directory
            dir_path = config.dr + "/" + directory
            DOI_NEW = DOI
            resp = config.api_target.destroy_dataset(DOI)
            if resp.status_code != 200:
                 logging.warning("main func: Cannot destroy dataset {}, status {} ".format(DOI, resp.status_code))

            dataset_versions = config.api_origin.get_dataset_versions(DOI)
            if dataset_versions.status_code != 200:
                logging.error("main func: Cannot get dataset versions for {} status {}".format(DOI, dataset_versions.status_code))
                continue
            versions = (dataset_versions.json()['data'])

            if len(versions) == 1:
                only = True
            else:
                only = False

            if not only:
                versions.sort(key=take_version)
            first = True
            for version in versions:
                cv = create_version(version, only, DOI, DOI_NEW, dir_path, total_files, correspondence, first)
                correspondence = cv[1]
                cv_status = cv[0]
                first = cv[2]
                if cv_status == False:
                    break
            if cv_status == False:
                logging.error("main func: Cannot create version for {}".format(DOI))
                continue
            now = datetime.now()
            logging.info("Dataset {} ended {} ".format(directory, now))
        except Exception as e:
            logging.error("Error with dataset {}, Error {}".format( directory, e))
        try:
            for t in total_files:
                all_total_files[t] = total_files[t]["new_id"]
        except Exception as e:
                print(e)

    with open('correspondence_old_new.json', 'w') as outfile:
        json.dump(correspondence, outfile, indent=4, sort_keys=True)
    with open('all_data_files.json', 'w') as outfile:
        json.dump(all_total_files, outfile, indent=4, sort_keys=True)
    now = datetime.now()
    logging.info("Program ended {}".format(now))

if __name__ == "__main__":
    main()
