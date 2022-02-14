import pyDataverse.utils as utils
from config import Config

config = Config()

def main():

    resp = config.api_origin.get_children(":root", "dataverse", ["dataverses", "datasets"])
    dataverses = utils.dataverse_tree_walker(resp)
    datasets = dataverses[1]
    print("Number of datasets ", len(datasets))
    print("Number of dataveres ", len(dataverses[0]))
    print(datasets)

    directories = []
    for dataset in datasets:
       doi = dataset['pid']
       index = doi.rfind("/")
       dr = doi[index+1:]
       directories.append(dr)
    print("Number of directories", len(directories) )
    print(directories)

if __name__ == "__main__":
    main()
