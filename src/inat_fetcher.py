from pyinaturalist import *
import pandas as pd
from pandas import json_normalize
from pyinaturalist_convert import *
from pyinaturalist import get_observations
from pandas import json_normalize
import requests
import os
import time


# These lines allows to make sure that we are placed at the repo directory level 

from pathlib import Path

p = Path(__file__).parents[1]
print(p)
os.chdir(p)



data_out_path = './data/out/'

output_filename = 'test_inat_output'
filename_suffix = 'csv'
path_to_output_file = os.path.join(data_out_path, output_filename + "." + filename_suffix)




response = get_observations(
    # user_id='pmallard',
    project_id=130644,
    page='all',
    per_page=300,
    access_token='eyJhbGciOiJIUzUxMiJ9.eyJ1c2VyX2lkIjoxMzc4MDMxLCJleHAiOjE2NTkyNjcwNjF9.DjYGEzNKm2DqRKAGJ07-_gAPTOY9vg5DJWYp-D4eV30ImLFgbPBEp04ipm-ZxzBz96iAo5Cb3fvX9Mm-xAxR2A'
)


# pprint(response)


df = to_dataframe(response)

df.info()

# Before exporting we move the id column to the beginning since it is needed to be at this position to be detected as a PK in airtbale or siomnilar dbs

# shift column 'id' to first position
first_column = df.pop('id')
  
# insert column using insert(position,column_name,
# first_column) function
df.insert(0, 'id', first_column)
  



# We keep the table 

df.to_csv(path_to_output_file, index = False)

