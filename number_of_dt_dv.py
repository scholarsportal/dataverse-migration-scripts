import pyDataverse.utils as utils
import requests

from config import Config

config = Config()

def dataverse_root_search():
    url = "https://dataverse.lib.umanitoba.ca/api/search?q=*&type=dataverse&subtree=um&&per_page=1000"
    token = config.api_token_origin
    head = {'X-Dataverse-key': '{}'.format(token)}
    response = requests.get(url, headers=head)
    dict = response.json()

    print(dict)

    dv = (dict['data']['items'])
    dv_id_lst = [x['identifier'] for x in dv]

    return(dv_id_lst)

def main():

    dv_list = dataverse_root_search()
    datasets = []
    for dv in dv_list:
        resp = config.api_origin.get_children(dv, "dataverse", ["dataverses", "datasets"])
        dataverses = utils.dataverse_tree_walker(resp)
        datasets += dataverses[1]
        datasets = [dict(t) for t in {tuple(d.items()) for d in datasets}]

    print("Number of datasets ", len(datasets))
    print("Number of dataverses ", len(dv_list))
    print(datasets)

    """
    directories = []
    for dataset in datasets:
       doi = dataset['pid']
       index = doi.rfind("/")
       dr = doi[index+1:]
       directories.append(dr)
    print("Number of directories", len(directories) )
    print(directories)
    """

if __name__ == "__main__":
    main()
