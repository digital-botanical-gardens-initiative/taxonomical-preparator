import pandas as pd
import json
from pandas import json_normalize
import sys
from opentree import OT
import shlex
import zipfile
import glob
import os
import subprocess



# Display option for pandas (optionnel)
# apparently buggy on Windows comment/uncomment if needed

# pd.set_option('display.max_rows', 60)
# pd.set_option("max_colwidth", 400)
# pd.set_option('precision', 5)


# %% First we'll externalise the variables we'll use in the rest of the script

switch_id = '9H11G243IaJTyjO'


data_in_path = '../data/in/'
data_out_path = '../data/out/'

input_filename = 'species_list_croisee'
filename_suffix = 'tsv'
path_to_input_file = os.path.join(data_in_path, input_filename + "." + filename_suffix)

treated_filename = 'species_list_treated'
filename_suffix = 'csv'
path_to_treated_file = os.path.join(data_out_path, treated_filename + "." + filename_suffix)






# the feature metadata table path is generated from the base bath to the GNPS results folder
feature_metadata_path = os.path.join(path_to_folder,'clusterinfo_summary','')


# Actually here we also want to have acces to the GNPS data
# %% Downloading Switch drive files

def switch_downloader(switch_id, path_to_file):
    """Downloads a switch file if given a swicth id
    Args:
            switch_id (string): a SWITCH Drive id
    Returns:
        nothing
    """

    switch_url = "https://drive.switch.ch/index.php/s/"+switch_id+"/download"

    cmd = 'wget '+switch_url+' -O '+ path_to_file
    subprocess.call(shlex.split(cmd))


# Cleaning "liste croisée"

# We download the file from switch

switch_downloader(switch_id='5Sfm98d6NcYqMgV',
                  path_to_file=path_to_input_file)

# %% 
# We now load the downloaded table file

species_list_df = pd.read_csv(path_to_input_file,
                       sep='\t', encoding= 'unicode_escape')



# Now we want to get the taxonomic information for each of the samples

# so we want to extract the organism information from the metadata file




# %%
# Set the lowertax columns header

org_column_header = 'Inventaire FRIBG'


species_list_df[org_column_header].dropna(inplace = True)
species_list_df[org_column_header]= species_list_df[org_column_header].str.lower()
# lets try to remove sp. in the fields
species_list_df[org_column_header] = species_list_df[org_column_header].str.replace(' sp' , '')
species_list_df[org_column_header] = species_list_df[org_column_header].str.replace(' x ' , ' ')


organisms = species_list_df[org_column_header].unique()
len_organisms = len(organisms)

print("%s unique organism have been selected from the metadata table." % len_organisms )
# %%
# We resolve the abovementionned species list using OpenTree https://github.com/OpenTreeOfLife/python-opentree

organisms_tnrs_matched = OT.tnrs_match(organisms, context_name=None, do_approximate_matching=True, include_suppressed=False)

# %%

organisms_tnrs_matched_filename = input_filename + '_organisms.json'

with open(organisms_tnrs_matched_filename, 'w') as out:
    sf = json.dumps(organisms_tnrs_matched.response_dict, indent=2, sort_keys=True)
    out.write('{}\n'.format(sf))


# %%
with open(organisms_tnrs_matched_filename) as tmpfile:
        jsondic = json.loads(tmpfile.read())

json_normalize(jsondic)
# %%

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

df_organism_tnrs_matched[df_organism_tnrs_matched['matched_name'] == df_organism_tnrs_matched['taxon.name']]

# %%


# We then want to match with the accepted name instead of the synonym in case both are present. 
# We thus order by matched_name and then by is_synonym status prior to returning the first row.

df_organism_tnrs_matched.sort_values(['search_string', 'is_synonym'], axis = 0, inplace = True)
df_organism_tnrs_matched_unique = df_organism_tnrs_matched.drop_duplicates('search_string', keep = 'first')

# both df are finally merged
merged_df = pd.merge(species_list_df, df_organism_tnrs_matched_unique, how='left', left_on='cleaned_sp', right_on='search_string', indicator=True)


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

#ott_list = ott_list[0:10]

# %%

taxon_info = []

for i in ott_list:
    query = OT.taxon_info(i, include_lineage=True)
    taxon_info.append(query)

# %%


tl = []

for i in taxon_info[:22]:
    with open('taxon_info.json', 'w') as out:
        tl.append(i.response_dict)
        yo = json.dumps(tl)
        out.write('{}\n'.format(yo))

# %%

with open("taxon_info.json") as tmpfile:
        jsondic = json.loads(tmpfile.read())

df = json_normalize(jsondic)



# %%

df_tax_lineage = json_normalize(jsondic,
               record_path=['lineage'],
               meta = ['ott_id', 'unique_name'],
               record_prefix='sub_',
               errors='ignore'
               )
# %%
# This keeps the last occurence of each ott_id / sub_rank grouping https://stackoverflow.com/a/41886945

df_tax_lineage_filtered = df_tax_lineage.groupby(['ott_id', 'sub_rank'], as_index=False).last()
# %%
#Here we pivot long to wide to get the taxonomy

df_tax_lineage_filtered_flat = df_tax_lineage_filtered.pivot(index='ott_id', columns='sub_rank', values='sub_name')

# %%
# Here we actually also want the lowertaxon (species usually) name

df_tax_lineage_filtered_flat = pd.merge(df_tax_lineage_filtered_flat, df_tax_lineage_filtered[['ott_id', 'unique_name']], how='left', on='ott_id', )

#Despite the left join ott_id are duplicated 

df_tax_lineage_filtered_flat.drop_duplicates(subset = ['ott_id', 'unique_name'], inplace = True)

# %%
# we keep the fields of interest

df_tax_lineage_filtered_flat[['ott_id', 'kingdom', 'phylum',
                              'class', 'order', 'family', 'genus', 'unique_name']]



# We now rename our columns of interest

renaming_dict = {'kingdom': 'query_otol_kingdom',
                 'phylum': 'query_otol_phylum',
                 'class': 'query_otol_class',
                 'order': 'query_otol_order',
                 'family': 'query_otol_family',
                 'genus': 'query_otol_genus',
                 'unique_name': 'query_otol_species'}


df_tax_lineage_filtered_flat.rename(columns=renaming_dict, inplace=True)

# We select columns of interest 

cols_to_keep = ['ott_id',
                'query_otol_kingdom',
                'query_otol_phylum',
                'query_otol_class',
                'query_otol_order',
                'query_otol_family',
                'query_otol_genus',
                'query_otol_species']

df_tax_lineage_filtered_flat = df_tax_lineage_filtered_flat[cols_to_keep]


# We merge this back with the samplemetadata only if we have an ott.id in the merged df 

samples_metadata = pd.merge(merged_df[pd.notnull(merged_df['taxon.ott_id'])], df_tax_lineage_filtered_flat, how='left', left_on='taxon.ott_id', right_on='ott_id' )
