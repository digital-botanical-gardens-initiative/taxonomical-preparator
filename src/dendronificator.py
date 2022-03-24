
# Loading required libs

import pandas as pd
import os
import sys
import pathlib
from pathlib import Path



p = Path(__file__).parents[1]
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


# This should be better https://stackoverflow.com/a/68349231


input_df.columns

input_df = input_df.head(100)

type(input_df.columns)

rootdir = pathlib.Path('./data/out/taxo')


taxo_folder = input_df.apply(lambda x: rootdir /
                                 str(x['organism_otol_kingdom']) /
                                 str(x['organism_otol_phylum']) /
                                 str(x['organism_otol_class']) /
                                 str(x['organism_otol_order']) /
                                 str(x['organism_otol_family']) /
                                 str(x['organism_otol_genus']) /
                                 str(x['organism_otol_species']) /
                                 f"{x['organism_otol_unique_name']}.md", axis='columns')

taxo_folder.drop_duplicates(keep='first', inplace=True)


for csvfile, data in input_df.groupby(taxo_folder):
    csvfile.parent.mkdir(parents=True, exist_ok=True)
    # data.to_csv(csvfile, index=False)
    ott_id = str(int(data['ott_id'].values))
    unique_name = str(
        data['organism_otol_unique_name'].values[0]).replace(' ', '_')
    print(ott_id)
    print(unique_name)
    # print(data['ott_id'])
    # with open(csvfile, "w") as text_file:
    #     print(f"Check OTOL tree here: https://tree.opentreeoflife.org/opentree/argus/opentree13.4@ott{ott_id}", file=text_file)
    #     print(f"\n", file=text_file)
    #     print(f"Check Wikipedia entry here: https://en.wikipedia.org/wiki/{unique_name}", file=text_file)
    otol_block = f'''
<html>
    <body>
    <iframe src="https://tree.opentreeoflife.org/opentree/argus/opentree13.4@ott{ott_id}"
    width="800" height="400" frameborder="0" allowfullscreen> </iframe>
    </body>
</html>
    '''
    wikipedia_block = f'''
<html>
    <body>
    <iframe src="https://en.wikipedia.org/wiki/{unique_name}"
    width="800" height="400" frameborder="0" allowfullscreen> </iframe>
    </body>
</html>
    '''
    otol_link = f'Direct link to OTOL entry: https://tree.opentreeoflife.org/opentree/argus/opentree13.4@ott{ott_id}'
    wikipedia_link = f'Direct link to Wikipedia entry: https://en.wikipedia.org/wiki/{unique_name}'

    with open(csvfile, "w") as text_file:
        print(f'This is the page dedicated to **{unique_name}**', file=text_file)
        print(f'\n', file=text_file)
        print(otol_link, file=text_file)
        print(f'\n', file=text_file)
        print(otol_block, file=text_file)
        print(f'\n', file=text_file)
        print(wikipedia_link, file=text_file)
        print(f'\n', file=text_file)
        print(wikipedia_block, file=text_file)

### We well check into formatted string literal to encode this

# https://stackoverflow.com/a/45624514/4908629


## We test the creation of files at upper levels


taxo_folder = input_df.apply(lambda x: rootdir /
                                 str(x['organism_otol_kingdom']) /
                                 str(x['organism_otol_phylum']) /
                                 str(x['organism_otol_class']) /
                                 str(x['organism_otol_order']) /
                                 f"{x['organism_otol_order']}.md", axis='columns')

taxo_folder.drop_duplicates(keep='first', inplace=True)


for csvfile, data in input_df.groupby(taxo_folder):
    csvfile.parent.mkdir(parents=True, exist_ok=True)
    # data.to_csv(csvfile, index=False)
    ott_id = str(int(data['ott_id'].values))
    unique_name = str(
        data['organism_otol_order'].values[0]).replace(' ', '_')
    print(ott_id)
    print(unique_name)
    # print(data['ott_id'])
    # with open(csvfile, "w") as text_file:
    #     print(f"Check OTOL tree here: https://tree.opentreeoflife.org/opentree/argus/opentree13.4@ott{ott_id}", file=text_file)
    #     print(f"\n", file=text_file)
    #     print(f"Check Wikipedia entry here: https://en.wikipedia.org/wiki/{unique_name}", file=text_file)
    otol_block = f'''
<html>
    <body>
    <iframe src="https://tree.opentreeoflife.org/opentree/argus/opentree13.4@ott{ott_id}"
    width="800" height="400" frameborder="0" allowfullscreen> </iframe>
    </body>
</html>
    '''
    wikipedia_block = f'''
<html>
    <body>
    <iframe src="https://en.wikipedia.org/wiki/{unique_name}"
    width="800" height="400" frameborder="0" allowfullscreen> </iframe>
    </body>
</html>
    '''
    otol_link = f'Direct link to OTOL entry: https://tree.opentreeoflife.org/opentree/argus/opentree13.4@ott{ott_id}'
    wikipedia_link = f'Direct link to Wikipedia entry: https://en.wikipedia.org/wiki/{unique_name}'

    with open(csvfile, "w") as text_file:
        print(f'This is the page dedicated to **{unique_name}**', file=text_file)
        print(f'\n', file=text_file)
        print(otol_link, file=text_file)
        print(f'\n', file=text_file)
        print(otol_block, file=text_file)
        print(f'\n', file=text_file)
        print(wikipedia_link, file=text_file)
        print(f'\n', file=text_file)
        print(wikipedia_block, file=text_file)

### We well check into formatted string literal to encode this

# https://stackoverflow.com/a/45624514/4908629

    