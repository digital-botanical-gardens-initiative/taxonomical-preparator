from pyinaturalist import *
import pandas as pd
from pandas import json_normalize
from pyinaturalist_convert import *
from pyinaturalist import get_observations
from pandas import json_normalize
import requests
import os, getpass
import time
import format_module
from dotenv import load_dotenv
import re
import numpy as np
import json
import argparse
load_dotenv()
import plotly
from tqdm import tqdm


parser = argparse.ArgumentParser(description='Fetch observations and save to a CSV file')
parser.add_argument('-o', '--path_to_output_file', help='Path to the output CSV file')
parser.add_argument('-p', '--project_id', type=int, help='Project ID')
args = parser.parse_args()

path_to_output_file = args.path_to_output_file
project_id = args.project_id

# These lines allows to make sure that we are placed at the repo directory level 

from pathlib import Path

p = Path(__file__).parents[1]
print(p)
os.chdir(p)


# data_out_path = './data/out/'

# output_filename = 'test_inat_output_current'
# filename_suffix = 'csv'
# path_to_output_file = os.path.join(data_out_path, output_filename + "." + filename_suffix)


# import env variable
access_token=os.getenv('ACCESS_TOKEN')

print(f"Fetching observations from iNat project id {project_id}. Depending on the number of observations, this may take a while, please be patient ! ")

response = get_observations(
    # user_id='pmallard',
    project_id=130644,
    page='all',
    per_page=300,
    #access_token=access_token
)

print("Here are the observations of the iNat project: ")
pprint(response)

type(response)


# Print number of observations
num_observations = len(response)
print(f"A total of {num_observations} observations were fetched from iNat project id {project_id}")


df = to_dataframe(response)

df.info()


# Before exporting we move the id column to the beginning since it is needed to be at this position to be detected as a PK in airtbale or siomnilar dbs

# shift column 'id' to first position
first_column = df.pop('id')
  
# insert column using insert(position,column_name,
# first_column) function
df.insert(0, 'id', first_column)

#formatting of data
format_module.location_formatting(df,'location','swiped_loc')
format_module.dbgi_id_extract(df)

# We keep the table 
print("Saving DataFrame to CSV...")
df.to_csv(path_to_output_file, index = False)
print("DataFrame saved to CSV.")

# Print file location
print("File saved at:", path_to_output_file)

