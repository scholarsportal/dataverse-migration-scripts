# dataverse-migration-scripts
Scripts for migrating datasets from one Dataverse installation to another
We are using python 3 and pyDataverse https://pydataverse.readthedocs.io/en/latest/
## config.json 
  It should be filled with information regarding source installation, target installation, api tokens etc. 
  1. "directories" should be an array of datasets in original dataverse that one wants to migrate, for example ['QBAYAF', 'KBHUA6'] with corresponding persistentId's doi:10.5072/FK2/QBAYAF, doi:10.5072/FK2/KBHUA.
  2. "api_token_origin" and "api_token_target" should be API tokens of super users of correspoding origin and target installations.
  3. "dataverse_alias" should be an alias of target dataverse.
  4. "dr" should be directory of files of original dataverse.
  5. "prefix_DOI" should be DOI prefix, for example "doi:10.5072/FK2".
  6. "db_name" should be a name of dataverse DB of target installation, for example "dvndb".
  7. "db_user" should be username of target DB, for example "dvnapp".
  8. "db_password" should be a password of "db_user"
  9. "db_host" should be hostname or ip address of target database. 
  10. db_port" should be port of target database.
  11. "builtin_users_key" should be BuiltinUsers.KEY from settings table of target installation.
  12. "password" should be initial password of new built in user that will be created in target installation.
## versions_files.py
It is main script for creating datasets with all the versons and files in the target installation. DOIs for published datasets will be preserved, for unpublished datasets new persistent identifiers will be created.
At the end script also creates corresponse between database ids of datasets of old installation and new installation. These ids are in created _correspondense_old_new.json_ file. Corresponse between ids of DataFiles of old installation and new installation will be saved in _all_data_files.json_. _versions_files.log_ is also created. 
## create_dataverses.py
This script creates dataverses in target dataverse (with dataverse_alias) from root dataverse of original installation. It also creates _dataverses.json_ file with all dataverses aliases and old ids of old installation.
## create_coorespondence.py
This script creates _correspondense.json_ file that containes correspondense between dataverses and datasets that belong to them.
## move_datasets.py
This script moves datasets created by _versions_files.py_ to dataverses created by _create_dataverses.py_. It uses _create_coorespondence.py_.
## update_dates_dataverses.py
This script updates dates for migrated dataverses in dvobject table of target installation. It uses _psycopg2-binary_ that can be installed by `pip install psycopg2-binary` 

It needs _dataverse_dates.json_ that can be generated from original installation using the following query:

`select id, createdate, modificationtime, publicationdate from dvobject where where dtype='Dataverse'` 

and convert it into json format.
## update_dates.py
This script updates dates for migrated datasets. It updates datasetversion table of target installation. It uses _correspondense_old_new.json_ that was created by _versions_files.py_.
## update_dates_dvobject.py
This script updates dates in dvobject for migrated datasets. It uses _correspondense_old_new.json_ that was created by _versions_files.py_.
## update_dates_datafiles.py
This scripts updates dates for migrated datafiles in dvobject table of target installation. It uses _all_data_files.json_ that was created by _versions_files.py_ and _datafile_dates.json_ that can be generated from original installation using  the following query:

`select id, createdate, modificationtime, publicationdate from dvobject where where dtype='DataFile'`

and convert it into json format.

For dates to take effect in targeted installation, solr reindexing in place should be performed.

`curl -X DELETE http://localhost:8080/api/admin/index/timestamps`

`curl http://localhost:8080/api/admin/index/continue`

This reindexing can be done at the end of migration process.

