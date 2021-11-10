#curl -H "X-Dataverse-key: $API_TOKEN" -X POST http://$SERVER/api/datasets/$id/move/$alias

from pyDataverse.api import NativeApi, DataAccessApi
import json
from config import Config

config = Config()

def find_correspondense(tree, parent, d):
    for data in tree:
        if data["type"] == "dataset":
            d[data['dataset_id']] = parent
        if data["type"] == "dataverse":
            if "children" in data:
                if len(data["children"]) > 0:
                    d = find_correspondense(data["children"], data["dataverse_alias"],d)

    return d

def main():
    resp = config.api_origin.get_children(":root", "dataverse", ["dataverses", "datasets"])
    print(resp)
    print(len(resp))

    d = {}
    d = find_correspondense(resp, ":root", d)
    print(d)
    print(len(d))
    with open('correspondense.json', 'w') as outfile:
        json.dump(d, outfile, indent=4, sort_keys=True)

if __name__ == "__main__":
    main()