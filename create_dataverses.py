import pyDataverse.utils as utils
import json
import requests
from xml.etree import ElementTree
from config import Config

config = Config()

def all_dataverses_alias():
    url = "https://dataverse.lib.umanitoba.ca/api/search?q=*&type=dataverse&subtree=um&&per_page=1000"
    token = config.api_token_origin
    head = {'X-Dataverse-key': '{}'.format(token)}
    response = requests.get(url, headers=head)

    dict = response.json()
    dv = (dict['data']['items'])
    dv_id_lst = [x['identifier'] for x in dv]
    return(dv_id_lst)

def get_dataverse_api_data(dataverse):
    url = "https://dataverse.lib.umanitoba.ca/api/v1/dataverses/" + dataverse
    token = config.api_token_origin
    head = {'X-Dataverse-key': '{}'.format(token)}
    response = requests.get(url, headers=head)
    dict = response.json()
    print(dict)
    return (dict['data'])

def find_dataverses(data, parent):
    dataverses = []
    if type(data) == list:
        for elem in data:
            dataverses+= find_dataverses(elem, parent)

    elif type(data) == dict:
        if data["type"] == "dataverse":
            dv = config.api_origin.get_dataverse(data['dataverse_id'])
            d =  dv.json()
            metadata = json.dumps(d['data'])
            if parent == 'mu':
                ds = {
                    "parent": config.dataverse_alias,
                    "child": data['dataverse_id']
                }
                try:
                    resp = config.api_target.create_dataverse(config.dataverse_alias, metadata)
                    dataverses.append(ds)
                except Exception as e:
                    print(e)
            else:
                ds = {
                    "parent": parent,
                    "child": data['dataverse_id']
                }
                try:
                    resp = config.api_target.create_dataverse(parent, metadata)
                    dataverses.append(ds)
                except Exception as e:
                    print(e)

        if "children" in data:
            if len(data["children"]) > 0:
                dataverses+= find_dataverses(data["children"], data['dataverse_alias'])

    return dataverses

def find_correspondence(tree, parent, d):
    for data in tree:
        if data["type"] == "dataset":
            d[data['pid']] = parent
        if data["type"] == "dataverse":
            if "children" in data:
                if len(data["children"]) > 0:
                    d = find_correspondence(data["children"], data["dataverse_alias"],d)

    return d

def main():
    mu_dataverse_collection = all_dataverses_alias()
    dataverses_added = []

    while len(mu_dataverse_collection) > 0:

        mu_dataverse = mu_dataverse_collection[0]

        print(mu_dataverse)

        mu_dataverse_data = get_dataverse_api_data(mu_dataverse)
        mu_dataverse_id = get_dataverse_api_data(mu_dataverse)["id"]
        mu_dataverse_owner_id = get_dataverse_api_data(mu_dataverse)["ownerId"]

        if mu_dataverse_owner_id == 1 or mu_dataverse_owner_id in dataverses_added:

            resp = config.api_origin.get_children(mu_dataverse, "dataverse", ["dataverses", "datasets"])
            child_dataverses = utils.dataverse_tree_walker(resp)
            dvs = child_dataverses[0]

            with open('dataverses.json', 'w') as outfile:
                json.dump(dvs, outfile, indent=4, sort_keys=True)

            resp = config.api_origin.get_dataverse(mu_dataverse_id)
            dv_metadata = resp.json()['data']

            try:
                if mu_dataverse_owner_id == 1:
                    resp = config.api_target.create_dataverse("manitoba", json.dumps(dv_metadata))
                else:
                    parent_alias = config.api_origin.dataverse_id2alias(mu_dataverse_owner_id)
                    resp = config.api_target.create_dataverse(parent_alias, json.dumps(dv_metadata))
            except Exception as e:
                print(resp.status_code)
                print(e)
                #continue

            url = config.base_url_origin + "/dvn/api/data-deposit/v1.1/swordv2/collection/dataverse/" + mu_dataverse
            print(url)
            req = requests.get(url, auth=(config.api_token_origin, ''))
            print(req.status_code)

            if resp.status_code == 200:
                root = ElementTree.fromstring(req.content)
                for child in root.iter("{http://purl.org/net/sword/terms/state}dataverseHasBeenReleased"):
                    if child.text == "true":
                        print("Publishing dataverse " + mu_dataverse)
                        try:
                            r = config.api_target.publish_dataverse(mu_dataverse)
                            print(r.status_code)
                        except:
                            pass
                    break

            else:
                print(mu_dataverse + " error getting publishing status " + str(req.status_code))

            dataverses_added.append(mu_dataverse_id)
            mu_dataverse_collection.pop(0)

        else:
            mu_dataverse_collection = mu_dataverse_collection[1:] + [mu_dataverse]

if __name__ == "__main__":
    main()
