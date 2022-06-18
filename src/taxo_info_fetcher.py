import pandas as pd 
from pandas import json_normalize
import requests
import os
import math
from taxo_resolver import *


# These lines allows to make sure that we are placed at the repo directory level 
from pathlib import Path

p = Path(__file__).parents[1]
os.chdir(p)


url = 'https://query.wikidata.org/sparql'

def wd_taxo_fetcher_from_ott(url, ott_id):
  
    query = '''
  PREFIX wdt: <http://www.wikidata.org/prop/direct/>
  SELECT ?ott ?wd ?img
  WHERE{{
      ?wd wdt:P9157 ?ott
      OPTIONAL{{ ?wd wdt:P18 ?img }}
      VALUES ?ott {{'{}'}}
  }}
  '''.format(ott_id)
  
    r = requests.get(url, params={'format': 'json', 'query': query})

    data = r.json()
    results = pd.DataFrame.from_dict(data).results.bindings
    df = json_normalize(results)
    return df 


def wd_taxo_fetcher_from_species(url, species_name):
  
    query = '''
  PREFIX wdt: <http://www.wikidata.org/prop/direct/>
  SELECT ?species_name ?wd ?img
  WHERE{{
      ?wd wdt:P225 ?species_name
      OPTIONAL{{ ?wd wdt:P18 ?img }}
      VALUES ?species_name {{'{}'}}
  }}
  '''.format(species_name)
  
    r = requests.get(url, params={'format': 'json', 'query': query})

    data = r.json()
    results = pd.DataFrame.from_dict(data).results.bindings
    df = json_normalize(results)
    return df 
  



data_in_path = './data/in/vgf_unaligned_data_test/'


path = os.path.normpath(data_in_path)
samples_dir = [directory for directory in os.listdir(path)]
df_list = []

for directory in samples_dir:
    metadata_path = os.path.join(path, directory, directory + '_metadata.tsv')
    try:
        metadata = pd.read_csv(metadata_path, sep='\t')
    except FileNotFoundError:
        continue
    except NotADirectoryError:
      continue
    path_to_results_folders = os.path.join(path, directory, '')
    if pd.isna(metadata['organism_species'][0]) == False :
      taxo_df = taxa_lineage_appender(metadata,'organism_species',True,path_to_results_folders,directory)
      wd_df = wd_taxo_fetcher_from_ott(url,taxo_df['ott_id'][0])
      print(wd_df['wd.value'][0])
      path_taxo_df = os.path.join(path_to_results_folders, directory + '_taxo_metadata.tsv')
      taxo_df.join(wd_df).to_csv(path_taxo_df, sep='\t')
    else:
      continue
    
    
# Manual taxo info fetching


species = ['Morinda citrifolia L.']


df = pd.DataFrame(species)


df.rename(columns={0: 'species'}, inplace=True)

path_to_results_folders = 'data/'
directory = 'manual'

taxa_lineage_appender(df,'species',True,path_to_results_folders,directory)




