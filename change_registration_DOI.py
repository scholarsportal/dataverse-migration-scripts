import pyDataverse.utils as utils
from config import Config

config = Config()

def main():
    resp = config.api_target.get_children(config.dataverse_alias, "dataverse",['dataverses','datasets'])

    dataverses = utils.dataverse_tree_walker(resp)
    datasets = dataverses[1]
    dvs = dataverses[0]
    print(len(dvs))
    print(len(datasets))

    for dataset in datasets:
         #resp = config.api_target.destroy_dataset(dataset['dataset_id'], False)
        url = config.base_url_target
        url += "/api/datasets/{0}/modifyRegistrationMetadata".format(dataset['dataset_id'])
        headers = {'X-Dataverse-key': config.api_token_target}
        r = config.api_target.post_request(url)
        print(url)
        print(r.json())


if __name__ == "__main__":
    main()