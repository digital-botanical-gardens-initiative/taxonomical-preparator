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




pd.set_option('display.max_rows', 60)
pd.set_option("max_colwidth", 400)
pd.set_option('precision', 5)


# %% First we'll externalise the variables we'll use in the rest of the script

switch_id = 'dnBosUs1ogKri2R'


data_in_path = '../data/in/'
data_out_path = '../data/out/'

base_filename = 'species_list'
filename_suffix = 'csv'
path_to_file = os.path.join(data_in_path, base_filename + "." + filename_suffix)




job_id = 'e2317a5aaa184e079cbd39235531dd9e'


gnps_job_path = '/Users/pma/Dropbox/Research_UNIGE/Projets/Ongoing/Leonie_fungi/'
project_name = 'leo_endophytic'
base_filename = 'GNPS_output_' + project_name
filename_suffix = 'zip'
path_to_folder = os.path.join(gnps_job_path, base_filename)
path_to_file = os.path.join(gnps_job_path, base_filename + "." + filename_suffix)


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

    cmd = 'wget '+switch_url+' -O '+path_to_file
    subprocess.call(shlex.split(cmd))


switch_downloader(switch_id='dnBosUs1ogKri2R',
                  path_to_file=path_to_file)

# %% 
# We now load the downloaded table file

species_list_df = pd.read_csv(path_to_file,
                       sep=';')


# We proceed to some data wrangling (here for example additional columns removal)

species_list_df = species_list_df[species_list_df.columns.drop(list(species_list_df.filter(regex='Unnamed: ')))]


# Now we want to get the taxonomic information for each of the samples

# so we want to extract the organism information from the metadata file




# %%
# Set the lowertax columns header

org_column_header = 'idTaxon'


species_list_df[org_column_header].dropna(inplace = True)
species_list_df[org_column_header]= species_list_df[org_column_header].str.lower()
# lets try to remove sp. in the fields
species_list_df[org_column_header] = species_list_df[org_column_header].str.replace(' sp' , '')
species_list_df[org_column_header] = species_list_df[org_column_header].str.replace(' x ' , ' ')

species_list_df['cleaned_genus'] = species_list_df[org_column_header].str.split().str[0]
species_list_df['cleaned_sp_epythet'] = species_list_df[org_column_header].str.split().str[1]
species_list_df['cleaned_sp'] = species_list_df['cleaned_genus'] + ' ' + species_list_df['cleaned_sp_epythet']


organisms = species_list_df['cleaned_sp'].unique()
len_organisms = len(organisms)

print("%s unique organism have been selected from the metadata table." % len_organisms )
# %%
# We first test on a subset
organisms = organisms[0:20]

organisms_tnrs_matched = OT.tnrs_match(organisms, context_name=None, do_approximate_matching=True, include_suppressed=False)

# %%

with open('organisms.json', 'w') as out:
    sf = json.dumps(organisms_tnrs_matched.response_dict, indent=2, sort_keys=True)
    out.write('{}\n'.format(sf))


# %%
with open("organisms.json") as tmpfile:
        jsondic = json.loads(tmpfile.read())

json_normalize(jsondic)
# %%

df_organism_tnrs_matched = json_normalize(jsondic,
               record_path=['results', 'matches']
               )



df_organism_tnrs_unmatched = json_normalize(jsondic,
               record_path=['unmatched_names']
               )
# %%

df_organism_tnrs_matched.info()


# %%
len(df_organism_tnrs_matched['taxon.ott_id'].unique())
# %%

df_organism_tnrs_matched[df_organism_tnrs_matched['is_synonym'] == False]
# %%

df_organism_tnrs_matched[df_organism_tnrs_matched['matched_name'] == df_organism_tnrs_matched['taxon.name']]

# %%

# First we remove the BK and QC because it appears to mess at the merge stage

# Lets not do it here 
# df_meta_samples = df_meta[df_meta['sample_type'] == 'sample']
# df_meta_notsamples = df_meta[df_meta['sample_type'] != 'sample']


# We then want to match with the accepted name instead of the synonym in case both are present. 
# We thus order by matched_name and then by is_synonym status prior to returning the first row.

df_organism_tnrs_matched.sort_values(['search_string', 'is_synonym'], axis = 0, inplace = True)
df_organism_tnrs_matched_unique = df_organism_tnrs_matched.drop_duplicates('search_string', keep = 'first')

# both df are finally merged
merged_df = pd.merge(df_meta, df_organism_tnrs_matched_unique, how='left', left_on=org_column_header, right_on='search_string', indicator=True)




# %%

# df_meta.info()
# df_organism_tnrs_matched.info()
# # %%

# # We output the number of unmatched taxon and check if the correspond to the unmatched OT outputs 

# unmatched_final = merged_df[merged_df['_merge'] == 'left_only']['organism_cof'].unique().tolist()

# unmatched_initial = df_organism_tnrs_unmatched[0].tolist()



# list(set(unmatched_final) - set(unmatched_initial))

# %%

# We drop the Unnamed : 0 Column
# and merge it back with the QC and Samples


# merged_df.drop('Unnamed: 0', axis = 1, inplace = True)
# merged_df.drop('taxon.synonyms', axis = 1, inplace = True)
# # %%
# cols = list(merged_df.columns.values)

# cols = ['taxon.unique_name',
# 'BARCODE',
#  'PLATESET',
#  'WELL',
#  'SUBSTANCE_NAME',
#  'Full_Species',
#  'Genus',
#  'Sp_alone',
#  'Species',
#  'Famille',
#  'Organe',
#  'MS_filename',
#  'corrected',
#  'broad_organ',
#  'tissue',
#  'subsystem',
#  'submitted_name',
#  'data_source_id',
#  'data_source_title',
#  'gni_uuid',
#  'matched_name2',
#  'taxon_id',
#  'edit_distance',
#  'imported_at',
#  'match_type',
#  'match_value',
#  'prescore',
#  'score_x',
#  'local_id',
#  'url',
#  'global_id',
#  'current_taxon_id',
#  'current_name_string',
#  'kingdom_cof',
#  'phylum_cof',
#  'class_cof',
#  'order_cof',
#  'family_cof',
#  'genus_cof',
#  'species_cof',
#  'subspecies_united',
#  'subspecies_cof_1',
#  'subspecies_cof_2',
#  'subspecies_cof_3',
#  'extracted_position',
#  'extracted_plate',
#  'inj_date',
#  'inj_order',
#  'sample_type',
#  'is_approximate_match',
#  'is_synonym',
#  'matched_name',
#  'nomenclature_code',
#  'score_y',
#  'search_string',
#  'taxon.flags',
#  'taxon.is_suppressed',
#  'taxon.is_suppressed_from_synth',
#  'taxon.name',
#  'taxon.ott_id',
#  'taxon.rank',
#  'taxon.source',
#  'taxon.tax_sources']

# merged_df = merged_df[cols]

# # we rename the columns 
# merged_df.rename(columns={'taxon.unique_name': 'featureid'}, inplace=True)


# merged_df['featureid'] = merged_df['featureid'].str.replace(' ','_')

# # %%
# #we eventually add some biological information at the feature level

# merged_df = pd.merge(merged_df, df_feature_meta_bio, how = 'left', on='MS_filename')


# # %%

# merged_df_deduped = merged_df.drop_duplicates('featureid')

# merged_df_deduped.dropna(subset = ['featureid'], inplace = True)


# full_df = pd.concat([merged_df, df_meta_notsamples])

/Users/pma/Dropbox/UniFr/Projects/Ongoing/Mycoscope/

project_path = '/Users/pma/Dropbox/UniFr/Projects/Ongoing/Mycoscope/'
metadata_otoled_name = '210415_duetz_species_list_single_sp_otoled.tsv'

merged_df.to_csv(project_path + metadata_otoled_name, sep = '\t', index = None)

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

for i in taxon_info:
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
               meta = ['ott_id', 'unique_name', 'rank'],
               record_prefix='sub_',
               errors='ignore'
               )


#%%

df_tax_lineage[df_tax_lineage['sub_rank'] == 'species']

# %%
# This keeps the last occurence of each ott_id / sub_rank grouping https://stackoverflow.com/a/41886945

df_tax_lineage_filtered = df_tax_lineage.groupby(['ott_id', 'sub_rank'], as_index=False).last()
# %%
#Here we pivot long to wide to get the taxonomy

df_tax_lineage_filtered_flat = df_tax_lineage_filtered.pivot(index='ott_id', columns='sub_rank', values='sub_name')

# %%
# Here we actually also want the lowertaxon (species usually) name

df_tax_lineage_filtered_flat = pd.merge(df_tax_lineage_filtered_flat, df_tax_lineage_filtered[['ott_id', 'unique_name', 'rank']], how='left', on='ott_id', )

#Despite the left join ott_id are duplicated 

df_tax_lineage_filtered_flat.drop_duplicates(subset = ['ott_id', 'unique_name'], inplace = True)

# %%
# we keep the fields of interest

df_tax_lineage_filtered_flat[['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'unique_name']]

# Here we have a problem sinc the 'unique_name' field is not necessarily the lowest taxon.
# Since we have the rank column which indicates the taxa level of the unique.name field we will fill the corresponding columns accordingly.

unique_names_pivoted = df_tax_lineage_filtered_flat.pivot(index='ott_id', columns='rank', values='unique_name')


 
# %%
# We merge it 

merged = pd.merge(df_tax_lineage_filtered_flat, unique_names_pivoted, how='left', left_on='ott_id', right_on='ott_id', indicator=False)


# We now fill the existing columns were NaN are present with this new dataset
# 
# 

cols_to_fill_to = ['class_x', 'family_x', 'genus_x']
cols_to_fill_from = ['class_y', 'family_y', 'genus_y']

for (col_to, col_from) in zip(cols_to_fill_to, cols_to_fill_from) : 
    merged[col_to] = merged[col_to].fillna(merged[col_from])


# %% 
# Now we do a littl bit of cleaning among these multiple columns

cols_to_keep = ['ott_id', 'class_x', 'domain', 'family_x', 'genus_x', 'kingdom',
                'order', 'phylum', 'unique_name', 'species']

merged = merged[cols_to_keep]

merged.rename(columns={'class_x': 'class',
                       'family_x': 'family',
                       'genus_x': 'genus'}, inplace=True)

df_tax_lineage_filtered_flat = merged


# %%

df_tax_lineage_filtered_flat['cat'] = 'k__' + df_tax_lineage_filtered_flat['kingdom'].map(
    str) \
+ '; p__' + \
df_tax_lineage_filtered_flat['phylum'].map(
    str) \
+ '; c__' + \
df_tax_lineage_filtered_flat['class'].map(
    str) \
+ '; o__' + \
df_tax_lineage_filtered_flat['order'].map(
    str) \
+ '; f__' + \
df_tax_lineage_filtered_flat['family'].map(
    str) \
+ '; g__' + \
df_tax_lineage_filtered_flat['genus'].map(
    str) \
+ '; s__' + \
df_tax_lineage_filtered_flat['species'].map(
    str)

# %%
# We now select the columns of interest to generate the feature_taxa table

feature_taxa = df_tax_lineage_filtered_flat[['unique_name', 'cat']]

# we rename the columns 
feature_taxa.rename(columns={'unique_name': 'featureid', 'cat': 'taxon',}, inplace=True)

# %%
# We eventuall complement with sample metadata 

feature_taxa = pd.merge(feature_taxa, merged_df, how='left', left_on='featureid', right_on='matched_name', indicator=False)


feature_taxa['featureid'] = feature_taxa['featureid'].str.replace(' ','_')

# For now we keep one feature-id since qiime will wine if we have multiple tree tips however this needs to be solved 

feature_taxa.drop_duplicates(['featureid'], inplace = True)

# and export this as a tsv file 

project_path = '~/Dropbox/Research_UNIGE/Projets/Ongoing/Leonie_fungi/'
feature_taxa_filename = 'feature_taxa.tsv'

feature_taxa.to_csv(project_path + feature_taxa_filename, sep = '\t', index = None)



# %%
# Lets retrieve a tree 

otol_tree = OT.synth_induced_tree(node_ids=None,
                           ott_ids=ott_list, label_format="name",
                           ignore_unknown_ids=True)




# %%
otol_tree.tree.print_plot(width=100)



otol_tree.tree



# %%

project_path = '~/Dropbox/Research_UNIGE/Projets/Ongoing/Leonie_fungi/'
tree_filename = 'first_duetz_otol_tree.tre'

otol_tree.tree.write(path = project_path + tree_filename, schema = "newick")
sys.stdout.write("Tree written to {}\n".format(tree_filename))


# %%

df_pl.head()
# %%
# we want to have a distinct prefix so as no to get mixed up with these numbers

df_pl = df_pl.add_prefix('feature_')
# %%

# now we also want to have (feature-id) which match the tips of our previously generated taxonomic tree that is Species name

df_pl['feature-id'] = df_pl.index

# both df are finally merged
informed_pl = pd.merge(df_pl, merged_df, how='left', left_on='feature-id', right_on='Name', indicator=False)

# %%

informed_pl.info
# %%

filter_col = [col for col in informed_pl if col.startswith('feature_')]
filter_col
# %%

informed_pl = informed_pl[['taxon.unique_name'] + filter_col]


# %%

informed_pl_mean = informed_pl.groupby('taxon.unique_name').mean()

informed_pl_mean.reset_index(inplace = True)

informed_pl_mean.rename(columns={'taxon.unique_name': 'feature-id'}, inplace=True)


informed_pl_mean['feature-id'] = informed_pl_mean['feature-id'].str.replace(' ','_')


# %%
# now we export this as a tsv

project_path = '~/Dropbox/Research_UNIGE/Projets/Ongoing/Leonie_fungi/'
feature_table_filename = 'feature_table.tsv'


informed_pl_mean.to_csv(project_path + feature_table_filename, sep = '\t', index = None)






# %% Now will need to work with the sample metadata (sample are metabolites)

# the feature table is loaded

df_feature_meta = pd.read_csv(feature_metadata_path + str(os.listdir(feature_metadata_path)[0]), sep='\t')


df_feature_meta.rename(columns={'cluster index': 'sample-id'}, inplace=True)

# We need to have the sample-id column at the first position for the table to be taken by qiime :

cols = ['sample-id', 'G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'GNPSLinkout_Cluster',
       'GNPSLinkout_Network', 'INCHI', 'LibraryID', 'MQScore', 'RTConsensus',
       'RTMean', 'RTStdErr', 'Smiles', 'SpectrumID', 'SumPeakIntensity',
       'UniqueFileSourcesCount','componentindex',
       'number of spectra', 'parent mass', 'precursor charge',
       'precursor mass', 'sum(precursor intensity)']

df_feature_meta = df_feature_meta[cols]

df_feature_meta['sample-id'] = 'feature_' + df_feature_meta['sample-id'].astype(str)


# %%
# and we export th table 

sample_metadata_filename = 'sample_metadata.tsv'

df_feature_meta.to_csv(project_path + sample_metadata_filename, sep = '\t', index = None)