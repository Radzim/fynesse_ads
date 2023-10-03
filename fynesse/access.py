# from .config import *
# import yaml

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import tables
import mongodb
import sqlite"""

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError

def download_files(path='data/'):
    filenames = [f'pp-{i}-part-{j}.csv' for j in [1, 2] for i in range(1995, 2021)]
    web_address = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/'
    print(filenames)
    # sql_upload_query = f"LOCAL DATA LOAD INFILE '{filename}' INTO TABLE '{table_name}' FIELDS TERMINATED BY ',' LINES STARTING BY '' TERMINATED BY '\n';"

def write_credentials(username, password):
    with open("credentials.yaml", "w") as file:
        credentials_dict = {'username': username,
                            'password': password}
        yaml.dump(credentials_dict, file)

def open_database(url="database-test.cgrre17yxw11.eu-west-2.rds.amazonaws.com", port=3306):
    database_details = {"url": url, "port": port}
    with open("credentials.yaml") as file:
        credentials = yaml.safe_load(file)
    username = credentials["username"]
    password = credentials["password"]
    url = database_details["url"]

write_credentials('admin', 'database-ads')