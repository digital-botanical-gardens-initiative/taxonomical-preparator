from pyinaturalist import *
import pandas as pd
from pandas import json_normalize
from pyinaturalist_convert import *



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


response = get_observations(
    project_id=130644,
    per_page=1000
)®

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