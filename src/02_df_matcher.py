
# Here we want to merge two datframe according to the string contents of specific columns

# Loading required libs

import pandas as pd
import os
import sys
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pandas import json_normalize
from tqdm import tqdm 




# defining the paths

data_in_path = './data/in/'
data_out_path = './data/out/'

input_filename = 'species_list_croisee'
filename_suffix = 'csv'
path_to_input_file = os.path.join(data_in_path, input_filename + "." + filename_suffix)

treated_filename = input_filename + '_treated'
filename_suffix = 'csv'
path_to_treated_file = os.path.join(data_out_path, treated_filename + "." + filename_suffix)


fuzzy_matched_filename = input_filename + '_fuzzy_matched'
filename_suffix = 'csv'
path_to_fuzzy_matched_file = os.path.join(data_out_path, fuzzy_matched_filename + "." + filename_suffix)


treated_taxo_filename = input_filename + '_treated_upper_taxo'
filename_suffix = 'csv'
path_to_treated_taxo_file = os.path.join(data_out_path, treated_taxo_filename + "." + filename_suffix)

merged_filename = input_filename + '_final'
filename_suffix = 'csv'
path_to_treated_merged_file = os.path.join(data_out_path, merged_filename + "." + filename_suffix)


# These lines allows to make sure that we are placed at the repo directory level 

from pathlib import Path

p = Path(__file__).parents[0]
print(p)
os.chdir(p)


# Loading the df

species_list_treated = pd.read_csv(path_to_treated_file,
                       sep=',', encoding= 'unicode_escape')


species_list_input = pd.read_csv('./data/in/species_list.csv',
                       sep=',', encoding= 'unicode_escape')


# we adapt the fuzzy matching scripts



# converting to pandas dataframes
dframe1 = species_list_treated
dframe2 = species_list_input
  
# empty lists for storing the matches later
mat1 = []
mat2 = []
p = []
  
# printing the pandas dataframes
print("First dataframe:\n", dframe1, 
      "\nSecond dataframe:\n", dframe2)
  
# converting dataframe column to list
# of elements
# to do fuzzy matching
list1 = dframe1['matched_name'].tolist()
list2 = dframe2['idTaxon'].tolist()

# Some cleaning on the lists
# we remove nan 

list1 = [x for x in list1 if str(x) != 'nan']
list2 = [x for x in list2 if str(x) != 'nan']

# we exploit the unique keys functionality when creating a dict to remove duplicated items in a list

list1 = list(dict.fromkeys(list1))
list2 = list(dict.fromkeys(list2))


# iterating through list1 to extract 
# it's closest match from list2
for i in tqdm(list2):
    mat1.append((i, process.extract(i, list1, limit=1)))

# In fact the above line could be chunked and sent in parralel

fuzzy_matched_df = pd.DataFrame(mat1)




# We use this dirty trick to expand the list of tuples in the results to column to two different columns: 
# see https://stackoverflow.com/questions/64702112/panda-expand-columns-with-list-into-multiple-columns

fuzzy_matched_df = pd.concat([fuzzy_matched_df.drop(columns=1), pd.DataFrame(fuzzy_matched_df[1].tolist(), index=fuzzy_matched_df.index).add_prefix(1)], 
               axis=1)

fuzzy_matched_df = pd.concat([fuzzy_matched_df.drop(columns='10'), pd.DataFrame(fuzzy_matched_df['10'].tolist(), index=fuzzy_matched_df.index).add_prefix(1)], 
               axis=1)

fuzzy_matched_df.info()


# We rename the newly created columns

fuzzy_matched_df.rename(columns={0: 'idTaxon', '10': 'matched_name', '11' : 'fuzzy_match_score'}, inplace=True)

# We observe that the same input can have various fuzzy matches (despite the n= 1 limit above). We thus proceeed to a fiultering step by 1. first ordering the df by fuzzy_score and then 2. selecting the first occurence


fuzzy_matched_df.sort_values(['idTaxon', 'fuzzy_match_score'], axis = 0, ascending =  False, inplace = True)


fuzzy_matched_unique = fuzzy_matched_df.drop_duplicates('idTaxon', keep = 'first')



fuzzy_matched_unique.to_csv(path_to_fuzzy_matched_file, sep = ',', index = None)



# We now proceed to df joins to fetch back the original metadata

merged_df = pd.merge(species_list_input, fuzzy_matched_unique, how='left', left_on='idTaxon', right_on='idTaxon')


# We open back the treated file

species_list_treated_taxo = pd.read_csv(path_to_treated_taxo_file,
                       sep=',', encoding= 'unicode_escape')



merged_df_all = pd.merge(merged_df, species_list_treated_taxo, how='left', left_on='matched_name', right_on='matched_name')


# We now output the cleaned table 


merged_df_all.to_csv(path_to_treated_merged_file, sep = ',', index = None)


# Alternatively (and in order not to repeat the full fuzzy matching stage ) we can append additional metadata 


# treated_merged_file = pd.read_csv(path_to_treated_merged_file,
#                        sep=',', encoding= 'unicode_escape')



# species_list_treated_taxo = pd.read_csv(path_to_treated_taxo_file,
#                        sep=',', encoding= 'unicode_escape')



# species_list_treated_taxo.filter(like='ott_id')



# pd.merge(treated_merged_file, species_list_treated_taxo.filter(like='ott_id'), how='left', on='ott_id' )




fuzzy_matched_unique = pd.read_csv(path_to_fuzzy_matched_file,
                       sep=',', encoding= 'unicode_escape')



# We now proceed to df joins to fetch back the original metadata

merged_df = pd.merge(species_list_input, fuzzy_matched_unique, how='left', left_on='idTaxon', right_on='idTaxon')


# We open back the treated file

species_list_treated_taxo = pd.read_csv(path_to_treated_taxo_file,
                       sep=',', encoding= 'unicode_escape')



merged_df_all = pd.merge(merged_df, species_list_treated_taxo, how='left', left_on='matched_name', right_on='matched_name')


# We now output the cleaned table 


merged_df_all.to_csv(path_to_treated_merged_file, sep = ',', index = None)
