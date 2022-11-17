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

load_dotenv()
import plotly


# These lines allows to make sure that we are placed at the repo directory level 

from pathlib import Path

p = Path(__file__).parents[1]
print(p)
os.chdir(p)


data_out_path = './data/out/'

output_filename = 'test_inat_output'
filename_suffix = 'csv'
path_to_output_file = os.path.join(data_out_path, output_filename + "." + filename_suffix)


# import env variable
access_token=os.getenv('ACCESS_TOKEN')


response = get_observations(
    # user_id='pmallard',
    project_id=130644,
    page='all',
    per_page=300,
    #access_token=access_token
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

#formatting
format_module.location_formatting(df,'location','swiped_loc')

format_module.dbgi_id_extract(df)
# We keep the table 

df.to_csv(path_to_output_file, index = False)


#update the database using update_db.py script
script = './src/update_db.py'
exec(open(script).read())



# Eventual plotlyy


# Using plotly.express
import plotly.express as px

df_1 = px.data.stocks()
fig = px.line(df_1, x='date', y="GOOG")
#Mfig.show()

df['count'] = 1

fig = px.line(df, x='time_observed_at', y="count")
#fig.show()



import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


fig = px.histogram(df, x="time_observed_at", y="count", histfunc="sum", title="Numbers of annotations at the daily")
fig.update_traces(xbins_size="M1")
fig.update_xaxes(showgrid=True, ticklabelmode="period", dtick="M1", tickformat="%b\n%Y")
fig.update_layout(bargap=0.1)
fig.add_trace(go.Scatter(mode="markers", x=df["time_observed_at"], y=df["count"], name="daily"))
#fig.show()

fig.write_json(file = 'data/out/inat.json')



# We fetch additional metadat from OTL

from opentree import OT





data_in_path = './data/in/'
data_out_path = './data/out/'

input_filename = 'inat_table'

filename_suffix = 'tsv'
path_to_input_file = os.path.join(data_in_path, input_filename + "." + filename_suffix)




organisms = df['taxon.name'].unique()
len_organisms = len(organisms)

print("%s unique organism have been selected from the metadata table." % len_organisms )
# %%
# We resolve the abovementionned species list using OpenTree https://github.com/OpenTreeOfLife/python-opentree

organisms_tnrs_matched = OT.tnrs_match(organisms, context_name=None, do_approximate_matching=True, include_suppressed=False)

# %%
# The above results are saved as a json file 

organisms_tnrs_matched_filename = data_out_path + input_filename + '_organisms.json'

with open(organisms_tnrs_matched_filename, 'w') as out:
    sf = json.dumps(organisms_tnrs_matched.response_dict, indent=2, sort_keys=True)
    out.write('{}\n'.format(sf))


# %%
# we reopen the json

with open(organisms_tnrs_matched_filename) as tmpfile:
        jsondic = json.loads(tmpfile.read())

json_normalize(jsondic)
# %%
# and here we normalize the json and output two  differents df for matched and unmatched results

df_organism_tnrs_matched = json_normalize(jsondic,
               record_path=['results', 'matches']
               )

df_organism_tnrs_matched_outfile = data_out_path + input_filename + '_tnrs_matched_outfile.csv'

df_organism_tnrs_matched.to_csv(df_organism_tnrs_matched_outfile, sep = ',', index = None)





df_organism_tnrs_unmatched = json_normalize(jsondic,
               record_path=['unmatched_names']
               )
df_organism_tnrs_unmatched_outfile = data_out_path + input_filename + '_tnrs_unmatched_outfile.csv'


df_organism_tnrs_unmatched.to_csv(df_organism_tnrs_unmatched_outfile, sep = ',', index = None)



# %%

df_organism_tnrs_matched.info()


# %%
len(df_organism_tnrs_matched['taxon.ott_id'].unique())
# %%

df_organism_tnrs_matched[df_organism_tnrs_matched['is_synonym'] == False]
# %%

# should be redundant with the above line ?

df_organism_tnrs_matched[df_organism_tnrs_matched['matched_name'] == df_organism_tnrs_matched['taxon.name']]

# %%


# We then want to match with the accepted name instead of the synonym in case both are present. 
# We thus order by matched_name and then by is_synonym status prior to returning the first row.

df_organism_tnrs_matched.sort_values(['search_string', 'is_synonym'], axis = 0, inplace = True)


df_organism_tnrs_matched_unique = df_organism_tnrs_matched.drop_duplicates('search_string', keep = 'first')

# both df are finally merged
merged_df = pd.merge(species_list_df, df_organism_tnrs_matched_unique, how='left', left_on=org_column_header, right_on='search_string', indicator=True)

# Duplicate are droppes
merged_df.drop_duplicates(subset=['Inventaire FRIBG', 'matched_name', 'taxon.ott_id'], keep='first', inplace=True, ignore_index=False)
# and nan also

merged_df.dropna(subset=['matched_name'], inplace = True)

merged_df.to_csv(path_to_treated_file, sep = ',', index = None)




# %%
#Now we want to retrieve the upper taxa lineage for all the samples

# Firsst we retrieve a list of unique ott_ids

# Here when checking the columns datatype we observe that the ott_ids are as float.
# We need to keep them as int
# displaying the datatypes 
display(merged_df.dtypes) 

# converting 'ott_ids' from float to int (check the astype('Int64') whic will work while the astype('int') won't see https://stackoverflow.com/a/54194908)
merged_df['taxon.ott_id'] = merged_df['taxon.ott_id'].astype('Int64')
  

# However, we then need to put them back to 
merged_df['taxon.ott_id']

ott_list = list(merged_df['taxon.ott_id'].dropna().astype('int'))
