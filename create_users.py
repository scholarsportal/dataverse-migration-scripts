import json
import requests
from config import Config

config = Config()

def main():

    url = config.base_url_target + '/api/builtin-users?key={0}&password={1}&sendEmailNotification=false'.format(config.builtin_users_key, config.password)
    print(url)
    headers = {"Content-type":"application/json"}
    with open('Alberta-list-users.txt') as f:
        data = json.load(f)
    count = 0
    for user in data['data']['users']:
        if "userIdentifier" in user:
            user['userName'] = user['userIdentifier']
            del user['userIdentifier']
        if "id" in user:
            del user['id']
        if "affiliation" not in user or user["affiliation"] == "":
            if "email" in user:
                email = user["email"]
                indx = email.find("@")
                if indx != -1:
                    ending = email[indx+1:].lower()
                    if ending == "ualberta.ca":
                        user["affiliation"] = "University of Alberta"
                    elif ending == "ucalgary.ca":
                        user["affiliation"] = "University of Calgary"
                    elif ending == "ubc.ca":
                        user["affiliation"] = "University of British Columbia"
                    elif ending == "utoronto.ca":
                        user["affiliation"] = "University of Toronto"
                    elif ending == "inrs.ca":
                        user["affiliation"] = "Institut national de la recherche scientifique"
                    elif ending == "macewan.ca":
                        user["affiliation"] = "MacEwan University"
                    elif ending == "mtroyal.ca":
                        user["affiliation"] = "Mount Royal University"
                    elif ending == "mcgill.ca":
                        user["affiliation"] = "McGill University"
                    elif ending == "uottawa.ca":
                        user["affiliation"] = "University of Ottawa"
                    else:
                        user["affiliation"] = "OTHER"
                else:
                    user["affiliation"] = "OTHER"
            else:
                user["affiliation"] = "OTHER"
        json_input = json.dumps(user)

        resp = requests.post(url, headers=headers, data=json_input)

        if resp.status_code == 200:
            count = count + 1
        else:
            print(resp.status_code)
            print(resp.json())
            print(user)

    print("Number of created users: " + str(count))
if __name__ == "__main__":
    main()