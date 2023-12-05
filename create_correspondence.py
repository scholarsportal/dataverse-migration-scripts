#curl -H "X-Dataverse-key: $API_TOKEN" -X POST http://$SERVER/api/datasets/$id/move/$alias
import requests
from pyDataverse.api import NativeApi, DataAccessApi
import json
from config import Config

config = Config()

def dataverse_root_search():
    url = "https://dataverse.lib.umanitoba.ca/api/search?q=*&type=dataverse&subtree=um&&per_page=1000"
    token = config.api_token_origin
    head = {'X-Dataverse-key': '{}'.format(token)}
    response = requests.get(url, headers=head)

    dict = response.json()
    dv = (dict['data']['items'])
    dv_id_lst = [x['identifier'] for x in dv]
    return (dv_id_lst)

def find_correspondence(tree, parent, d):
    for data in tree:
        if data["type"] == "dataset":
            d[data['dataset_id']] = parent
        if data["type"] == "dataverse":
            if "children" in data:
                if len(data["children"]) > 0:
                    d = find_correspondence(data["children"], data["dataverse_alias"],d)

    return d

def main():

    dv_list = dataverse_root_search()
    d = {}
    for dv in dv_list:
        resp = config.api_origin.get_children(dv, "dataverse", ["dataverses", "datasets"])
        sub_d = find_correspondence(resp, dv, d)
        d[next(iter(sub_d))] = next(iter(sub_d.values()))
    with open('correspondence.json', 'w') as outfile:
        json.dump(d, outfile, indent=4, sort_keys=True)

if __name__ == "__main__":
    main()
