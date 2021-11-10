import json
import requests
from config import Config

config = Config()

def add_corr(url, url_target, headers, headers_group_creation, alias, alias_target):
    url_ds = url + alias + '/groups'
    url_ds_target = url_target + alias_target + '/groups/'
    resp = config.api_origin.get_request(url_ds)
    if resp.status_code == 200 and len(resp.json()['data']) > 0:
        print("Dataverse " + alias)
        print(resp.json()['data'])
        for g in resp.json()['data']:
            groupAlias = g['groupAliasInOwner']
            url_ds_target_group = url_ds_target + groupAlias
            resp = requests.get(url_ds_target_group, headers=headers)
            print(resp.status_code)
            print(resp.json())

def main():
    with open('dataverses.json') as f:
        data = json.load(f)

    correspondence = {}
    url = config.base_url_origin + '/api/dataverses/'
    url_target = config.base_url_target + '/api/dataverses/'
    headers = {'X-Dataverse-key': config.api_token_target}
    headers_group_creation = {'X-Dataverse-key': config.api_token_target, "Content-type":"application/json"}
    for dv in data:
        alias = dv['dataverse_alias']
        alias_target = alias


        url_ds = url + alias + '/groups'
        url_ds_target = url_target + alias_target + '/groups/'
        resp = config.api_origin.get_request(url_ds)
        if resp.status_code == 200 and len(resp.json()['data']) > 0:
            print("Dataverse " + alias)
            print(resp.json()['data'])
            for g in resp.json()['data']:
                groupAlias = g['groupAliasInOwner']
                url_ds_target_group = url_ds_target + groupAlias
                print(url_ds_target_group)
                resp_target = requests.get(url_ds_target_group, headers=headers)
                print(resp_target.status_code)
                print(resp_target.json())
                print(groupAlias)
                print(g['identifier'])
                print(resp_target.json()['data']['identifier'])
                correspondence[g['identifier']] = resp_target.json()['data']['identifier']

    print(correspondence)
    with open('correspondence_explicit_groups.json', 'w') as outfile:
        json.dump(correspondence, outfile, indent=4, sort_keys=True)

if __name__ == "__main__":
    main()
