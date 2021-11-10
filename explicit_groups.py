import json
import requests
from config import Config

config = Config()

def add_permission(url, url_target, headers, headers_group_creation, alias, alias_target):
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
            print(groupAlias)
            print(resp.status_code)
            if resp.status_code == 404:
                print("Not created yet")
                print(g)
                group = {}
                if 'description' in g:
                    group['description'] = g['description']
                if 'displayName' in g:
                    group["displayName"] = g['displayName']
                if 'groupAliasInOwner' in g:
                    group['aliasInOwner'] = g['groupAliasInOwner']
                if 'containedRoleAssignees' in g:
                    containedRoleAssignees = g['containedRoleAssignees']

                json_input = json.dumps(group)
                print(url_ds_target)
                resp = requests.post(url_ds_target, headers=headers_group_creation, data=json_input)
                print(resp.status_code)
                print(resp.json())
                if resp.status_code == 201:
                    url_role = url_ds_target_group + "/roleAssignees"
                    print(url_role)
                    roles = {"roleAssignees": containedRoleAssignees}
                    print(roles)
                    resp = requests.post(url_role, headers=headers_group_creation, data=json.dumps(containedRoleAssignees))
                    print(resp.status_code)
                    print(resp.json())

def main():
    with open('dataverses.json') as f:
        data = json.load(f)
    print(data)
    url = config.base_url_origin + '/api/dataverses/'
    url_target = config.base_url_target + '/api/dataverses/'
    headers = {'X-Dataverse-key': config.api_token_target}
    headers_group_creation = {'X-Dataverse-key': config.api_token_target, "Content-type":"application/json"}
    for dv in data:
        alias = dv['dataverse_alias']
        add_permission(url, url_target, headers, headers_group_creation, alias, alias)

    #for root
    #add_permission(url, url_target, headers, headers_group_creation, ':root', config.dataverse_alias)

if __name__ == "__main__":
    main()
