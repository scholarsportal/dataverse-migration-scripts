import json
import requests
from config import Config

config = Config()

def add_roles(url, alias, alias_target, url_target, groups):
    url_ds = url + alias + '/assignments'
    url_ds_target = url_target + alias_target + '/assignments'
    print(url_ds)
    resp = config.api_origin.get_request(url_ds)
    print(resp.status_code)
    if resp.status_code == 200:
        roles = resp.json()['data']
        print(resp.json())
        resp_target = config.api_target.get_request(url_ds_target)
        if resp_target.status_code == 200:
            print(resp_target.json()['data'])
            roles_target = resp_target.json()['data']
        else:
            roles_target = []
        # curl -H X-Dataverse-key:$API_TOKEN -X POST -H "Content-Type: application/json" $SERVER_URL/api/dataverses/$ID/assignments --upload-file role.json
        headers = {"Content-type": "application/json", "X-Dataverse-key": config.api_token_target}

        all_roles_target = {}
        for role in roles_target:
            r_target = ""
            if 'assignee' in role:
                if role['assignee'] in groups:
                    r_target = r_target + groups[role['assignee']]
                else:
                    r_target = role['assignee']
            if '_roleAlias' in role:
                r_target = r_target + " " + role['_roleAlias']
            all_roles_target[r_target] = r_target

        print("Roles target")
        print(all_roles_target)
        print("Roles origin")

        for role in roles:
            r_original = ""
            print(role)
            if 'assignee' in role:
                if role['assignee'] in groups:
                    r_original = r_original + groups[role['assignee']]
                    role['assignee'] = groups[role['assignee']]
                else:
                    r_original = r_original + role['assignee']
            print(role)
            if '_roleAlias' in role:
                role['role'] = role['_roleAlias']
                r_original = r_original + " " + role['_roleAlias']
                del role['_roleAlias']
            if 'id' in role:
                del role['id']
            if 'roleId' in role:
                del role['roleId']
            if 'definitionPointId' in role:
                del role['definitionPointId']

            print(r_original)
            if r_original in all_roles_target:
                print("Not add")
                continue
            else:
                json_input = json.dumps(role)
                try:
                    print('+++')
                    print(json_input)
                    print('---')
                    resp = requests.post(url_ds_target, headers=headers, data=json_input)
                    print(resp.json())
                except Exception as e:
                    print("Error {}, filename {}".format(e, role))
                print(resp.status_code)
    else:
        print(url_ds + "  cannot get role")

def main():
    with open('dataverses.json') as f:
        data = json.load(f)
    with open('correspondence_explicit_groups.json') as explicit:
        groups = json.load(explicit)
    url = config.base_url_origin + '/api/dataverses/'
    url_target = config.base_url_target + '/api/dataverses/'
    for dv in data:
         alias = dv['dataverse_alias']
         print(alias)
         add_roles(url, alias, alias, url_target, groups)

    #for root dataverse :root #If added then inheritance
    #add_roles(url, ":root", config.dataverse_alias, url_target)

if __name__ == "__main__":
    main()