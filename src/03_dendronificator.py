
# Loading required libs

import pandas as pd
import numpy as np
import math
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
path_to_input_file = os.path.join(
    data_out_path, input_filename + "." + filename_suffix)


# Loading the df

input_df = pd.read_csv(path_to_input_file,
                       sep=',', encoding='unicode_escape')


# We want to generate a set of empty text files appropriately formatted
# so that they will be later on taken by Dendron and will reflect the taxonomy
# See markdown import


# This should be better https://stackoverflow.com/a/68349231


input_df.columns

input_df.info()

# #We convert the floats to int to avoid the ValueError: cannot convert float NaN to integer error
# input_df['ott_id'] = input_df['ott_id'].astype('Int64')


# input_df = input_df.head(100)
def split_dataframe(df, chunk_size = 10000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

splitted_df_list = split_dataframe(input_df, chunk_size = 3000)

# Beware here we work only on a subset. This was set because of problems with massive markdown import from Dendron in VSCode
# it is hacked by working on smaller chunks. Make sure to repeat for the full taxo !
input_df = splitted_df_list[0]

# len(splitted_df_list)

# type(input_df.columns)

rootdir = pathlib.Path('./data/out/taxo/taxo-jbuf')


# As decribed here https://github.com/digitized-botanical-gardens-initiative/taxonomical-preparator/issues/3
# we need to remove . 

input_df['organism_otol_unique_name'] = input_df['organism_otol_unique_name'].str.replace('.', '')




taxo_folder = input_df.apply(lambda x: rootdir /
                             str(x['organism_otol_kingdom']) /
                             str(x['organism_otol_phylum']) /
                             str(x['organism_otol_class']) /
                             str(x['organism_otol_order']) /
                             f"{x['organism_otol_unique_name']}.md" if x['rank'] == 'family'
                             else (rootdir /
                                   str(x['organism_otol_kingdom']) /
                                   str(x['organism_otol_phylum']) /
                                   str(x['organism_otol_class']) /
                                   str(x['organism_otol_order']) /
                                   str(x['organism_otol_family']) /
                                   f"{x['organism_otol_unique_name']}.md" if x['rank'] == 'genus'
                                   else (rootdir /
                                         str(x['organism_otol_kingdom']) /
                                         str(x['organism_otol_phylum']) /
                                         str(x['organism_otol_class']) /
                                         str(x['organism_otol_order']) /
                                         str(x['organism_otol_family']) /
                                         str(x['organism_otol_genus']) /
                                         f"{x['organism_otol_unique_name']}.md" if x['rank'] == 'species'
                                         else (
                                             rootdir /
                                             str(x['organism_otol_kingdom']) /
                                             str(x['organism_otol_phylum']) /
                                             str(x['organism_otol_class']) /
                                             str(x['organism_otol_order']) /
                                             str(x['organism_otol_family']) /
                                             str(x['organism_otol_genus']) /
                                             str(x['organism_otol_species']) /
                                             f"{x['organism_otol_unique_name']}.md" if x['rank'] == 'subspecies' or ['rank'] == 'varietas'
                                             else
                                             rootdir /
                                             f"{x['organism_otol_unique_name']}.md"
                                         )
                                         )
                                   ), axis='columns')


taxo_folder.drop_duplicates(keep='first', inplace=True)

for csvfile, data in input_df.groupby(taxo_folder):
    csvfile.parent.mkdir(parents=True, exist_ok=True)
    # data.to_csv(csvfile, index=False)
    if math.isnan(data['ott_id']) != True:
        ott_id = str(int(data['ott_id'].values))
    else:
        ott_id = data['ott_id']
    unique_name = str(
        data['organism_otol_unique_name'].values[0]).replace(' ', '_')
    wd_taxa_qid = str(
        data['wd_taxa_qid'].values[0])
    print(ott_id)
    print(unique_name)
    print(wd_taxa_qid)
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
    wikidata_block = f'''
<html>
 <body>
  <iframe src="https://query.wikidata.org/embed.html#SELECT%20%3Fmesh_id%20%3Fmesh_idLabel%20%3Fchemical_compound%20%3Fchemical_compoundLabel%20%3Fqueried_taxa%20%20%3Fqueried_taxaLabel%20%3Fqueried_taxall%20%3Fqueried_taxallLabel%20%3Freference%20%3FreferenceLabel%20WHERE%20%7B%0A%20%20VALUES%20%3Fchemical_classes%20%7B%0A%20%20%20%20wd%3AQ11173%20%23%20chemical%20compound%0A%20%20%20%20wd%3AQ59199015%20%23%20group%20of%20stereoisomers%0A%20%20%7D%0A%20%20%3Fchemical_compound%20wdt%3AP31%20%3Fchemical_classes.%20%23%20We%20select%20instance%20of%20the%20chemical%20classes%20%28chemical%20compound%20or%20group%20of%20stereoisomers%29%0A%20%20VALUES%20%3Fqueried_taxa%20%7B%0A%20%20%20%20wd%3A{wd_taxa_qid}%0A%20%20%20%20%23Enter%20the%20Wikidata%20identifier%20of%20your%20taxa%20of%20interest%20%28here%20Streptomyces%20coelicolor%29.%0A%20%20%20%20%23%20You%20can%20remove%20the%20Qxxxxxxx%20id%20and%20hit%20Ctrl%2Bspace%2C%20thype%20in%20the%20first%20letters%20and%20it%20should%20autocomplete%0A%20%20%7D%0A%20%20%7B%0A%20%20%20%20%3Fchemical_compound%20p%3AP703%20%3Fstmt.%23%20We%20selecte%20chemical%20classes%20having%20the%20found%20in%20taxon%20statement%0A%20%20%20%20%3Fqueried_taxall%20wdt%3AP171%2a%20%3Fqueried_taxa.%0A%20%20%20%20%3Fstmt%20ps%3AP703%20%3Fqueried_taxall.%20%23%20and%20the%20restrict%20the%20found%20in%20taxon%20statement%20to%20match%20our%20queried%20taxa%0A%20%20%20%20%23%3Fchemical_compound%20p%3AP2868%20%3Fmesh.%0A%20%20%20%20%23%3Fmesh%20ps%3AP2868%20%3Fmesh_id.%0A%20%20%7D%0A%20%20%20%20%20%20OPTIONAL%20%7B%0A%20%20%20%20%20%20%3Fstmt%20prov%3AwasDerivedFrom%20%3Fref.%0A%20%20%20%20%20%20%3Fref%20pr%3AP248%20%3Freference.%20%23%20We%20optionally%20return%20the%20reference%20if%20present%20stated%20in%0A%20%20%20%20%7D%0A%20%20%0A%20%0A%20%20SERVICE%20wikibase%3Alabel%20%7B%20bd%3AserviceParam%20wikibase%3Alanguage%20%22%5BAUTO_LANGUAGE%5D%2Cen%22.%20%7D%0A%7D" 
   width="800" height="400" frameborder="0" allowfullscreen></iframe>
 </body>
</html>
    '''
    otol_link = f'Direct link to OTOL entry: https://tree.opentreeoflife.org/opentree/argus/opentree13.4@ott{ott_id}'
    wikipedia_link = f'Direct link to Wikipedia entry: https://en.wikipedia.org/wiki/{unique_name}'

    with open(csvfile, "w") as text_file:
        print(
            f'This is the page dedicated to **{unique_name}**', file=text_file)
        print(f'\n', file=text_file)
        print(f'## OTOL Taxonomy', file=text_file)
        print(f'\n', file=text_file)
        print(otol_link, file=text_file)
        print(f'\n', file=text_file)
        print(otol_block, file=text_file)
        print(f'\n', file=text_file)
        print(f'## Wikipedia entry of the taxa', file=text_file)
        print(f'\n', file=text_file)
        print(wikipedia_link, file=text_file)
        print(f'\n', file=text_file)
        print(wikipedia_block, file=text_file)
        print(f'\n', file=text_file)
        print(f'## Metabolites found in the taxa', file=text_file)
        print(f'\n', file=text_file)
        print(wikidata_block, file=text_file)

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
    if math.isnan(data['organism_otol_order_ott_id']) != True:
        ott_id = str(int(data['organism_otol_order_ott_id'].values))
    else:
        ott_id = data['organism_otol_order_ott_id']
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
        print(
            f'This is the page dedicated to **{unique_name}**', file=text_file)
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


taxo_folder = input_df.apply(lambda x: rootdir /
                             str(x['organism_otol_kingdom']) /
                             str(x['organism_otol_phylum']) /
                             str(x['organism_otol_class']) /
                             f"{x['organism_otol_class']}.md", axis='columns')

taxo_folder.drop_duplicates(keep='first', inplace=True)


for csvfile, data in input_df.groupby(taxo_folder):
    csvfile.parent.mkdir(parents=True, exist_ok=True)
    # data.to_csv(csvfile, index=False)
    if math.isnan(data['organism_otol_class_ott_id']) != True:
        ott_id = str(int(data['organism_otol_class_ott_id'].values))
    else:
        ott_id = data['organism_otol_class_ott_id']
    unique_name = str(
        data['organism_otol_class'].values[0]).replace(' ', '_')
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
        print(
            f'This is the page dedicated to **{unique_name}**', file=text_file)
        print(f'\n', file=text_file)
        print(otol_link, file=text_file)
        print(f'\n', file=text_file)
        print(otol_block, file=text_file)
        print(f'\n', file=text_file)
        print(wikipedia_link, file=text_file)
        print(f'\n', file=text_file)
        print(wikipedia_block, file=text_file)

