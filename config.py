from pyDataverse.api import NativeApi, DataAccessApi
import json

class Config:

    def __init__(self):
        with open('config.json') as f:
            conf = json.load(f)
        self.base_url_origin = conf['base_url_origin']
        self.api_token_origin = conf['api_token_origin']

        self.base_url_target = conf['base_url_target']
        self.api_token_target = conf['api_token_target']

        self.dataverse_alias = conf['dataverse_alias']
        self.dr =  conf['dr']
        self.prefix_DOI = conf['prefix_DOI']

        self.directories = conf['directories']

        self.api_origin = NativeApi(self.base_url_origin, self.api_token_origin)
        self.data_api_origin = DataAccessApi(self.base_url_origin, self.api_token_origin)

        self.api_target = NativeApi(self.base_url_target, self.api_token_target)
        self.data_api_target = DataAccessApi(self.base_url_target, self.api_token_target)

        #database
        self.db_name = conf['db_name']
        self.db_user = conf['db_user']
        self.db_password = conf['db_password']
        self.db_host = conf['db_host']
        self.db_port = conf['db_port']

        #users
        self.builtin_users_key = conf['builtin_users_key']
        self.password = conf['password']
