# dataverse-migration-scripts

Scripts for migrating datasets from one [Dataverse repository](https://dataverse.org) installation to another. These scripts were used to migrated datasets from version 4.20 (University of Alberta) to version 5.1 (Scholars Portal). For these scripts to work with other versions, updates may be needed.

We are using Python 3 and [pyDataverse](https://pydataverse.readthedocs.io/en/latest/).

## DOIs

If the installation you are migrating datasets from is minting DOIs, those DOIs will need to be migrated from the Datacite account of the previous installation to the new one. If the target installation's Datacite account does not own the DOIs for the datasets your are migrating, you will not be able to publish the datasets in the new installation.

## config.json

The config file contains information used in the scripts.

* `base_url_origin`: the base URL of the source installation.
* `base_url_target`: the base URL of the target installation.
* `api_token_origin` and `api_token_target`: the API tokens of superuser accounts of corresponding origin and target installations.
* `dataverse_alias`: an alias of the target dataverse datasets will be migrated into.
* `dr`: the directory of files of original dataverse.
* `prefix_DOI`: the DOI prefix for migrated datasets, for example "doi:10.5072/FK2".
* `db_name`: the name of the dataverse DB of target installation, for example "dvndb".
* `db_user`: the username of target DB, for example "dvnapp".
* `db_password`: the password of `db_user`.
* `db_host`: the hostname or ip address of target database.
* `db_port`: the port of target database.
* `builtin_users_key`: the [BuiltinUsers.KEY](https://guides.dataverse.org/en/5.1/installation/config.html#builtinusers-key) from the settings table of the target installation.
* `password`: the initial password of new built in users that will be created in the target installation.
* `directories`: an array of all datasets in the original dataverse that you want to migrate, for example `['QBAYAF', 'KBHUA6']` would be entered for datasets with the persistentId's of doi:10.5072/FK2/QBAYAF and doi:10.5072/FK2/KBHUA.

## Scripts

Each script for the migration is described below, and are included in the order that they should be run for the migration.

### number_of_dt_dv.py

This is an optional script that can be used to fill the "directories" field in `config.json`. It lists all of the datasets in the root dataverse of the original installation recursively.

### versions_files.py

This is main script for creating datasets with all the versions and files in the target installation. DOIs for published datasets will be preserved, and for unpublished datasets new persistent identifiers will be created.

At the end the script also creates correspondence between database ids of datasets from the old installation and the new installation. These ids are in created `correspondence_old_new.json` file. Correspondence between ids of DataFiles of the old installation and new installation will be saved in `all_data_files.json`. `versions_files.log` is also created by this script.

### create_dataverses.py

This script creates dataverses in the target dataverse (with dataverse_alias) from the root dataverse of the original installation. It also creates `dataverses.json` file with all dataverses aliases and old ids of old installation.

### create_correspondence.py

This script creates `correspondence.json` file that contains correspondence between dataverses and datasets that belong to them.

### move_datasets.py

This script moves datasets created by `versions_files.py` to dataverses created by `create_dataverses.py`. It uses `create_correspondence.py`.

### update_dates_dataverses.py

This script updates dates for migrated dataverses in the dvobject table of target installation. It uses `psycopg2-binary` that can be installed with `pip install psycopg2-binary`.

It needs the `dataverse_dates.json` file that can be generated from the original installation using the following query:

```sql
select id, createdate, modificationtime, publicationdate from dvobject where where dtype='Dataverse'
```

...and convert it into JSON format.

### update_dates.py

This script updates dates for migrated datasets. It updates the datasetversion table of the target installation. It uses `correspondence_old_new.json` that was created by `versions_files.py`.

### update_dates_dvobject.py

This script updates dates in the dvobject table for migrated datasets. It uses `correspondence_old_new.json` that was created by `versions_files.py`.

### update_dates_datafiles.py

This script updates dates for migrated datafiles in dvobject table of target installation. It uses `all_data_files.json` that was created by `versions_files.py` and `datafile_dates.json` that can be generated from original installation using the following query:

```sql
select id, createdate, modificationtime, publicationdate from dvobject where where dtype='DataFile'
```

...and convert it into JSON format.

For dates to take effect in the target installation, solr re-indexing in place should be performed:

```bash
curl -X DELETE http://localhost:8080/api/admin/index/timestamps
curl http://localhost:8080/api/admin/index/continue
```

This re-indexing can be done at the end of migration process.

### create_users.py

This script creates builtin users in the target installation. This script has customized affiliations set for importing users which can be modified or removed. It uses `users.json` that contains list of all the users from original installation. `users.json` can be created from the original installation using:

```bash
curl -H "X-Dataverse-key:$API_TOKEN" "$SERVER_URL/api/admin/list-users?itemsPerPage=1000"
```

### explicit_groups.py

This script creates explicit groups and assign them to dataverses. It uses the `dataverses.json` file that was created by `create_dataverses.py`.

### correspondence_explicit_groups.py

This script creates the `correspondence_explicit_groups.json` file that contains the correspondence between explicit groups of old and new installations.

### role_dataverses.py

This script assigns roles for dataverses. It uses the `dataverses.json` file that was created by `create_dataverses.py`, and the `correspondence_explicit_groups.json` file that was created by `correspondence_explicit_groups.py`.

### role_datasets.py

This script assigns roles for datasets. It uses the `dataverses.json` file that was created by `create_dataverses.py`, and the `correspondence_explicit_groups.json` file that was created by `correspondence_explicit_groups.py`.

### change_registration_DOI.py

This script updates DOI registration metadata with Datacite to ensure the updated dates are reflected.

### Delete cached files

The last step of migration is deleting all _.cached_ files of the datasets that were migrated in the filesystem. This is done so that the updated dates are reflected in the exported metadata files.
