#curl -H "X-Dataverse-key: $API_TOKEN" -X POST http://$SERVER/api/datasets/$id/move/$alias

import json
from config import Config

config = Config()

def main():
    with open('correspondence.json') as f:
        dataset_dataverse = json.load(f)
    print(dataset_dataverse)
    with open('correspondence_old_new.json') as f:
        old_new_datasets = json.load(f)
    print(old_new_datasets)
    url = config.base_url_target + '/api/datasets/'
    for old_id in old_new_datasets:
        new_id = old_new_datasets[old_id]
        dataverse = dataset_dataverse[old_id]
        if dataverse == "um":
            dataverse = config.dataverse_alias
        print(str(new_id) + " " + dataverse )
        url_move = url + str(new_id) + "/move/" + dataverse
        resp = config.api_target.post_request(url_move)
        print(resp.status_code)

if __name__ == "__main__":
    main()
