import pyDataverse.utils as utils
import json
import requests
from xml.etree import ElementTree
from config import Config

config = Config()

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
            if parent == ':root':
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

def find_correspondense(tree, parent, d):
    for data in tree:
        if data["type"] == "dataset":
            d[data['pid']] = parent
        if data["type"] == "dataverse":
            if "children" in data:
                if len(data["children"]) > 0:
                    d = find_correspondense(data["children"], data["dataverse_alias"],d)

    return d

def main():
    resp = config.api_origin.get_children(":root", "dataverse", ["dataverses", "datasets" ])

    dataverses = utils.dataverse_tree_walker(resp)

    dvs = dataverses[0]
    with open('dataverses.json', 'w') as outfile:
        json.dump(dvs, outfile, indent=4, sort_keys=True)

    for dv in dvs:
        resp = config.api_origin.get_dataverse(dv['dataverse_id'])
        dv_metadata = resp.json()['data']
        owner_id = dv_metadata['ownerId']
        if owner_id != 1:
            parent_alias = config.api_origin.dataverse_id2alias(owner_id)
        else:
            parent_alias = config.dataverse_alias
        try:
            resp = config.api_target.create_dataverse(parent_alias, json.dumps(dv_metadata))
        except Exception as e:
            print(resp.status_code)
            print(e)
            continue
        url = config.base_url_origin +   "/dvn/api/data-deposit/v1.1/swordv2/collection/dataverse/" + dv['dataverse_alias']
        print(url)
        req = requests.get(url, auth=(config.api_token_origin, ''))
        print(req.status_code)

        if req.status_code == 200:
            root = ElementTree.fromstring(req.content)
            for child in root.iter("{http://purl.org/net/sword/terms/state}dataverseHasBeenReleased"):
                if child.text == "true":
                    print("Publishing dataverse " + dv['dataverse_alias'])
                    r = config.api_target.publish_dataverse(dv['dataverse_alias'])
                    print(r.status_code)
                break
        else:
            print(dv['dataverse_alias'] + " error getting publishing status " + req.status_code)
if __name__ == "__main__":
    main()