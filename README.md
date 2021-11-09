# dataverse-migration-scripts
Scripts for migrating datasets from one Dataverse installation to another
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
  
