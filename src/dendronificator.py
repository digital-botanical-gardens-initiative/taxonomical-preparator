
# Loading required libs

import pandas as pd
import os
import sys
from pathlib import Path



p = Path(__file__).parents[0]
print(p)
os.chdir(p)



# defining the paths

data_in_path = './data/in/'
data_out_path = './data/out/'

input_filename = 'species_list_croisee_final'
filename_suffix = 'csv'
path_to_input_file = os.path.join(data_out_path, input_filename + "." + filename_suffix)



# Loading the df

input_df = pd.read_csv(path_to_input_file,
                       sep=',', encoding= 'unicode_escape')


# We want to generate a set of empty text files appropriately formatted 
# so that they will be later on taken by Dendron and will reflect the taxonomy
# See markdown import 


# Testing here a row export as text files (for dendron loading)
# found https://stackoverflow.com/a/28377334

for x in beh.head(10).iterrows():
    #iterrows returns a tuple per record whihc you can unpack
    # X[0] is the index
    # X[1] is a tuple of the rows values so X[1][0] is the value of the first column etc.
    pd.DataFrame([x[1][0]]).to_csv(str(x[1][1])+".txt", header=False, index=False)

beh.head(10).iterrows()[1]


data_out_path_md = data_out_path + 'md/'

for x in beh.head(100).iterrows():
    #iterrows returns a tuple per record whihc you can unpack
    # X[0] is the index
    # X[1] is a tuple of the rows values so X[1][0] is the value of the first column etc.
    pd.DataFrame([x[1][0]]).to_csv(data_out_path_md + ".".join([str(x[1][1]), str(x[1][2]), str(x[1][3]), str(x[1][4]), str(x[1][5]), str(x[1][6]), str(x[1][7]), str(x[1][8])]) +".md", header=False, index=False)


This should be better https://stackoverflow.com/a/68349231