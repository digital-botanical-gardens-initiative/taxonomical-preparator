
# Here we want to merge two datframe according to the string contents of specific columns

# Loading required libs

import pandas as pd
import os
import sys


# defining the paths

data_in_path = './data/in/'
data_out_path = './data/out/'

input_filename = 'species_list_croisee'
filename_suffix = 'csv'
path_to_input_file = os.path.join(data_in_path, input_filename + "." + filename_suffix)

treated_filename = input_filename + '_treated'
filename_suffix = 'csv'
path_to_treated_file = os.path.join(data_out_path, treated_filename + "." + filename_suffix)



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



# We test https://stackoverflow.com/a/60092339

pat = '|'.join(r"\b{}\b".format(x) for x in species_list_treated['Inventaire FRIBG'])

species_list_input['sp']= species_list_input['idTaxon'].str.extract('('+ pat + ')', expand=False)



# new data frame with split value columns 
species_list_treated["sp_listed"] = species_list_treated["Inventaire FRIBG"].str.split(" ", n = 1, expand = True) 


# new data frame with split value columns 
species_list_input["sp_listed"] = species_list_input['idTaxon'].str.split(" ", expand = True) 


# df display 
data 


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
list1 = dframe1['Inventaire FRIBG'].tolist()
list2 = dframe2['idTaxon'].tolist()
  
# taking the threshold as 82
threshold = 82
  
# iterating through list1 to extract 
# it's closest match from list2
for i in list1:
    mat1.append(process.extract(i, list2, limit=1))
dframe1['matches'] = mat1
  
# iterating through the closest matches
# to filter out the maximum closest match
for j in dframe1['matches']:
    for k in j:
        if k[1] >= threshold:
            p.append(k[0])
    mat2.append(",".join(p))
    p = []
  
  
# storing the resultant matches back to dframe1
dframe1['matches'] = mat2
print("\nDataFrame after Fuzzy matching:")
dframe1


