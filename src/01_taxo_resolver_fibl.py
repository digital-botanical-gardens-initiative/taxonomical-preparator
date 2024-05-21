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
from dotenv import load_dotenv



# Display option for pandas (optionnel)
# apparently buggy on Windows comment/uncomment if needed

# pd.set_option('display.max_rows', 60)
# pd.set_option("max_colwidth", 400)
# pd.set_option('precision', 5)


# These lines allows to make sure that we are placed at the repo directory level 

from pathlib import Path

p = Path(__file__).parents[1]
print(p)
os.chdir(p)

# import env variable
load_dotenv()

switch_id=os.getenv('SWITCH_ID')

# %% First we'll externalise the variables we'll use in the rest of the script


data_in_path = './data/in/'
data_out_path = './data/out/fibl/'

input_filename = 'metadata_final'
filename_suffix = 'tsv'
path_to_input_file = os.path.join(data_in_path, input_filename + "." + filename_suffix)

treated_filename = input_filename + '_treated'
filename_suffix = 'csv'
path_to_treated_file = os.path.join(data_out_path, treated_filename + "." + filename_suffix)

taxo_filename = input_filename + '_upper_taxo'
filename_suffix = 'csv'
path_to_taxo_file = os.path.join(data_out_path, taxo_filename + "." + filename_suffix)


treated_taxo_filename = input_filename + '_treated_upper_taxo'
filename_suffix = 'csv'
path_to_treated_taxo_file = os.path.join(data_out_path, treated_taxo_filename + "." + filename_suffix)




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

# switch_downloader(switch_id='5Sfm98d6NcYqMgV',
#                   path_to_file=path_to_input_file)

# %% 
# We now load the downloaded table file

species_list_df = pd.read_csv(path_to_input_file,
                       sep='\t', encoding= 'unicode_escape')



# Now we want to get the taxonomic information for each of the samples

# so we want to extract the organism information from the metadata file




# %%
# Set the lowertax columns header

# define here the header for the species
org_column_header = 'source_taxon'


species_list_df[org_column_header].dropna(inplace = True)
species_list_df[org_column_header]= species_list_df[org_column_header].str.lower()
# We remove sp. and x (varietas) in the fields Normally should not be required in this case
species_list_df[org_column_header] = species_list_df[org_column_header].str.replace(r' sp' , '', regex=True)
species_list_df[org_column_header] = species_list_df[org_column_header].str.replace(r' x ' , ' ', regex=True)
species_list_df[org_column_header] = species_list_df[org_column_header].str.replace(r' x$' , '', regex=True)


organisms = species_list_df[org_column_header].unique()
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
# we repopne the json

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
merged_df.drop_duplicates(subset=['source_taxon', 'matched_name', 'taxon.ott_id'], keep='first', inplace=True, ignore_index=False)
# and nan also

merged_df.dropna(subset=['matched_name'], inplace = True)

merged_df.to_csv(path_to_treated_file, sep = ',', index = None)




# %%
#Now we want to retrieve the upper taxa lineage for all the samples

# First we retrieve a list of unique ott_ids

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

taxon_info_filename = data_out_path + input_filename + '_taxon_info.json'

tl = []

for i in taxon_info:
    with open(taxon_info_filename, 'w') as out:
        tl.append(i.response_dict)
        yo = json.dumps(tl)
        out.write('{}\n'.format(yo))

# %%

with open(taxon_info_filename) as tmpfile:
        jsondic = json.loads(tmpfile.read())

df = json_normalize(jsondic)



# %%

df_tax_lineage = json_normalize(jsondic,
               record_path=['lineage'],
               meta = ['ott_id', 'unique_name', 'flags', 'rank'],
               record_prefix='sub_',
               errors='ignore'
               )
# %%
# This keeps the last occurence of each ott_id / sub_rank grouping https://stackoverflow.com/a/41886945

df_tax_lineage_filtered = df_tax_lineage.groupby(['ott_id', 'sub_rank'], as_index=False).last()
# %%
#Here we pivot long to wide to get the taxonomy

df_tax_lineage_filtered_flat = df_tax_lineage_filtered.pivot(index='ott_id', columns='sub_rank', values='sub_name')

# Since we also want the ott_id for each taxo levels we repeat this operation

df_tax_lineage_filtered_flat_ott_ids = df_tax_lineage_filtered.pivot(index='ott_id', columns='sub_rank', values='sub_ott_id')
# We now prefix all column names of this df

df_tax_lineage_filtered_flat_ott_ids = df_tax_lineage_filtered_flat_ott_ids.add_suffix('_ott_id')

# We return all ott_id as int

df_tax_lineage_filtered_flat_ott_ids = df_tax_lineage_filtered_flat_ott_ids.astype('Int64')


# %%
# Here we actually also want the lowertaxon (species usually) name

df_tax_lineage_filtered_flat = pd.merge(df_tax_lineage_filtered_flat, df_tax_lineage_filtered[['ott_id', 'unique_name']], how='left', on='ott_id' )




#Despite the left join ott_id are duplicated 

df_tax_lineage_filtered_flat.drop_duplicates(subset = ['ott_id', 'unique_name'], inplace = True)

df_tax_lineage_filtered_flat.columns


# %%
# And here we add the df containing the ott_ids for each upper taxo levels


df_tax_lineage_filtered_flat_full =  pd.merge(df_tax_lineage_filtered_flat, df_tax_lineage_filtered_flat_ott_ids, how='left', on='ott_id' )


df_tax_lineage_filtered_flat_full.columns





# %%
# we keep the fields of interest

# sub_df = df_tax_lineage_filtered_flat[['ott_id', 'kingdom', 'phylum',
#                               'class', 'order', 'family', 'genus', 'species', 'unique_name']]

sub_df = df_tax_lineage_filtered_flat_full

# [['ott_id', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus',
#                                        'species', 'unique_name', 'kingdom_ott_id', 'phylum_ott_id', 'class_ott_id',
#                                        'order_ott_id', 'family_ott_id', 'genus_ott_id', 'species_ott_id']
#                                       ]


sub_df.drop_duplicates(keep='first', inplace = True)


# Here we observe that we loose the information concerning the rank of the lowest taxon.
# we actually can fetch it back by merging via ott_id with the initial df

# Since we get a TypeError: unhashable type: 'list' when trying to drop duplicate on the subsetted df we first need to convert the flags
# columns to a tuple (which is hashable)

df_tax_lineage_filtered["flags_tuple"] = df_tax_lineage_filtered['flags'].apply(lambda x: tuple(x))


df_tax_lineage_filtered_sub = df_tax_lineage_filtered.drop_duplicates(subset=['ott_id', 'flags_tuple', 'rank'], keep='first', ignore_index=False)

# Save df_tax_lineage_filtered_sub to csv


merged = pd.merge(sub_df, df_tax_lineage_filtered_sub[['ott_id', 'flags', 'rank']], how='inner', on='ott_id')



# Create the new columns based on the rank and populate them accordingly
for index, row in merged.iterrows():
    rank = row['rank']
    unique_name_col = f"{rank}"
    ott_id_col = f"{rank}_ott_id"
    
    # Assign the unique_name to the new column
    merged.loc[index, unique_name_col] = row['unique_name']
    # Assign the ott_id to the new column
    merged.loc[index, ott_id_col] = row['ott_id']


# Here, for each row we fetch the corresponding rank in the rank column and create a new column with this rank. We populate this column with the unique_name value.





# We now rename our columns of interest

renaming_dict = {'kingdom': 'organism_otol_kingdom',
                 'phylum': 'organism_otol_phylum',
                 'class': 'organism_otol_class',
                 'order': 'organism_otol_order',
                 'family': 'organism_otol_family',
                 'genus': 'organism_otol_genus',
                 'species': 'organism_otol_species',
                 'unique_name': 'organism_otol_unique_name',
                 'kingdom_ott_id': 'organism_otol_kingdom_ott_id_ott_id',
                 'phylum_ott_id': 'organism_otol_phylum_ott_id',
                 'class_ott_id': 'organism_otol_class_ott_id',
                 'order_ott_id': 'organism_otol_order_ott_id',
                 'family_ott_id': 'organism_otol_family_ott_id',
                 'genus_ott_id': 'organism_otol_genus_ott_id',
                 'species_ott_id': 'organism_otol_species_ott_id',
                 }


merged.rename(columns=renaming_dict, inplace=True)

merged.to_csv(path_to_taxo_file, sep = ',', index = None)


# We open back the treated file

species_list_treated = pd.read_csv(path_to_treated_file,
                       sep=',', encoding= 'unicode_escape')


# We merge this back with the samplemetadata only if we have an ott.id in the merged df 

species_list_treated_upper_taxo = pd.merge(species_list_treated, merged, how='left', left_on='taxon.ott_id', right_on='ott_id' )


# We keep the resulting file : 

species_list_treated_upper_taxo.to_csv(path_to_treated_taxo_file, sep = ',', index = None)


