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

output_filename = 'wd_inat_output'
filename_suffix = 'csv'
path_to_output_file = os.path.join(data_out_path, output_filename + "." + filename_suffix)




observations = get_observations(user_id='pmallard')

for obs in observations['results']:
    pprint(obs)
    
counts = get_observation_species_counts(user_id='pmallard', quality_grade='research')
pprint(counts)

histogram = get_observation_histogram(user_id='pmallard')
pprint(histogram)


response = get_observations(
    taxon_name='Danaus plexippus',
    created_on='2020-08-27',
    photos=True,
    geo=True,
    geoprivacy='open',
    place_id=7953
)

pprint(response)


# Following this answer https://github.com/pyinat/pyinaturalist/issues/403






response = get_observations(
    #user_id='pmallard',
    project_id=130644,
    per_page=1000
)


response = get_observations(
    # user_id='pmallard',
    project_id=130644,
    page='all',
    per_page=200,
    access_token='eyJhbGciOiJIUzUxMiJ9.eyJ1c2VyX2lkIjoxMzc4MDMxLCJleHAiOjE2NTkyNjcwNjF9.DjYGEzNKm2DqRKAGJ07-_gAPTOY9vg5DJWYp-D4eV30ImLFgbPBEp04ipm-ZxzBz96iAo5Cb3fvX9Mm-xAxR2A'
)

get_observations()

print(response)

pprint(response)


df = to_dataframe(response)

df.infos

# Before exporting we move the id column to the beginning since it is needed to be at this position to be detected as a PK in airtbale or siomnilar dbs

# shift column 'id' to first position
first_column = df.pop('id')
  
# insert column using insert(position,column_name,
# first_column) function
df.insert(0, 'id', first_column)
  



df.to_csv('test.csv', index = False)


json_normalize(df)

metadata = df[['ofvs']]

json_normalize(metadata, record_path=['ofvs'])




### Reading outputs of the inaturalist API

#curl -X GET --header 'Accept: application/json' 'https://api.inaturalist.org/v1/observations.csv?project_id=130644&order=desc&order_by=created_at'

import json

hip = pd.read_json('~/dbgi_observations.json',orient='records')

json.loads('~/dbgi_observations.json')
json_normalize(json.loads(hip), record_path=['results'])


# we also compare with the iNat export tool

https://www.inaturalist.org/observations/export


observations_inat = pd.read_csv('~/Downloads/observations-247283.csv')

# We merge the csv download and the pyinat output 

# both df are finally merged
merged_df = pd.merge(observations_inat, df, how='left', left_on='id', right_on='id', indicator=True)

merged_df.to_csv('~/Dropbox/git_repos/COMMONS_Lab/DBGI/taxonomical-preparator/data/out/dbgi_pyinat_inat_merged.csv')


#### 
# Our aim here is to fetch all observation from a project, then the corresponding taxon ids and then query wikidata LOTUS for related informations


# We retrieve all observation ids of a project

response = get_observations(
    project_id=130644,
    page='all',
    per_page=200,
    access_token='eyJhbGciOiJIUzUxMiJ9.eyJ1c2VyX2lkIjoxMzc4MDMxLCJleHAiOjE2NTkyNjcwNjF9.DjYGEzNKm2DqRKAGJ07-_gAPTOY9vg5DJWYp-D4eV30ImLFgbPBEp04ipm-ZxzBz96iAo5Cb3fvX9Mm-xAxR2A'
)


pprint(response)


df = to_dataframe(response)

# We then remove duplicated taxon.id 

df.drop_duplicates(subset=['taxon.id'], keep='first', inplace=True, ignore_index=False)


# We now set up a wd query 


url = 'https://query.wikidata.org/sparql'

def wd_taxo_fetcher_from_species(url, species_name):
  
    query = '''
  PREFIX wdt: <http://www.wikidata.org/prop/direct/>
  SELECT ?species_name ?wd
  WHERE{{
      ?wd wdt:P225 ?species_name
      VALUES ?species_name {{'{}'}}
  }}
  '''.format(species_name)
  
    r = requests.get(url, params={'format': 'json', 'query': query})

    data = r.json()
    results = pd.DataFrame.from_dict(data).results.bindings
    df = json_normalize(results)
    return df 
 
def wd_taxo_fetcher_from_inatid(url, inat_id):
    
    query = '''
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    SELECT ?species_name ?inat_id ?wd
    WHERE{{
        ?wd wdt:P225 ?species_name;
            wdt:P3151 ?inat_id.
        VALUES ?inat_id {{'{}'}}
    }}
    '''.format(inat_id)

    r = requests.get(url, params={'format': 'json', 'query': query})

    data = r.json()
    results = pd.DataFrame.from_dict(data).results.bindings
    df = json_normalize(results)
    return df 

def wd_chemotaxo_fetcher_from_inatid(url, inat_id):
    
    query = '''
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    SELECT DISTINCT ?chemical_compound ?chemical_compoundLabel ?smiles_canonical ?inchi ?species_name ?inat_id ?species ?reference ?referenceLabel
    WHERE{{
        ?chemical_compound wdt:P235 ?inchikey.
        OPTIONAL {{ ?chemical_compound wdt:P233 ?smiles_canonical. }}
        OPTIONAL {{ ?chemical_compound wdt:P234 ?inchi. }}
        {{
            ?chemical_compound p:P703 ?stmt.
            ?stmt ps:P703 ?species.
        }}
        OPTIONAL {{
            ?stmt prov:wasDerivedFrom ?ref.
            ?ref pr:P248 ?reference.
        }}
        ?species wdt:P225 ?species_name;
            wdt:P3151 ?inat_id.
        VALUES ?inat_id {{'{}'}}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    '''.format(inat_id)

    r = requests.get(url, params={'format': 'json', 'query': query})

    data = r.json()
    results = pd.DataFrame.from_dict(data).results.bindings
    df = json_normalize(results)
    return df 



# appended_data = []

# for inatid in df['taxon.id'].head(10) :
#     wd_df = wd_taxo_fetcher_from_inatid(url,inatid)
#     time.sleep(1)
#     appended_data.append(wd_df)
#     print(inatid)
# # see pd.concat documentation for more info
# wd_df_concat = pd.concat(appended_data)





appended_data = []

for inatid in df['taxon.id'] :
    wd_df = wd_chemotaxo_fetcher_from_inatid(url,inatid)
    time.sleep(1)
    appended_data.append(wd_df)
    print(inatid)
# see pd.concat documentation for more info
wd_df_concat = pd.concat(appended_data)


# Some cleaning 
# We drop here all columns with the following string in their name :lang .type

df = wd_df_concat[wd_df_concat.columns.drop(list(wd_df_concat.filter(regex=':lang|.type')))]

# and columns reordering


df = df[['inat_id.value', 'species.value', 'species_name.value',
       'chemical_compound.value', 'chemical_compoundLabel.value', 
       'smiles_canonical.value', 'inchi.value', 
       'reference.value', 'referenceLabel.value']]

# We keep the table 

df.to_csv(path_to_output_file, index = False)