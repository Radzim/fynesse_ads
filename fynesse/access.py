from .config import *

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

def download_files():
    filenames = [f'pp-{i}-part-{j}' for j in [1, 2] for i in range(1995, 2021)]
    print(filenames)
    # sql_upload_query = f"LOCAL DATA LOAD INFILE '{filename}' INTO TABLE '{table_name}' FIELDS TERMINATED BY ',' LINES STARTING BY '' TERMINATED BY '\n';"

