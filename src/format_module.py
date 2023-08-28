'''Module to format inaturalist data'''



'''Function to create new formated location column'''
def location_formatting(df,old_column,new_column):
    # Create a new column with all values initialized to 'NA'
    df[new_column] = 'NA'

    # Use the apply method to process each row in the dataframe
    df[new_column] = df[old_column].apply(lambda x: (x[1], x[0]))
    
    # Return the modified dataframe
    return df



'''Function to extract the dbgi id from the ofvs column and put it in a new column'''
'''def dbgi_id_extract(df):
    import numpy as np

    #Initialize the new column with NAs
    df['emi_external_id'] = np.nan
    
    #set the ofvs column as a string type
    df.ofvs = df.ofvs.astype(str)

    #loop over each line
    for i in range(len(df)):
        #initialize an empty dictionary
        dct = dict()
        #if the ofvs line is not empty
        if df.ofvs[i] != '[]':
            #format the data
            ls = df.ofvs[i].replace(' ','').replace('\'','') 
            #for each value separated by a ,
            # separate again by ':' and set the key of the dictionnary to be the part before ':'
            # and the value to be the part after ':' 
            for j in ls.split(','):
                j = j.split(':')
                dct[j[0]] = j[1]
            #change the value of the emi_external_id to be the value of the 'value_ci' key 
            # when the 'name_ci' of the dictionary equal 'emi_external_id'
            #This avoids the problem of having the wrong 'value_ci' taken
            if dct['name_ci'] == 'emi_external_id':
                df.at[i,'emi_external_id'] = dct['value_ci']'''

def dbgi_id_extract(df):
    import numpy as np

    # Define a function to extract the emi_external_id from the ofvs list
    def extract_emi_external_id(lst):
        for d in lst:
            if d['name_ci'] == 'emi_external_id':
                return d['value_ci']
        return np.nan

    # Apply the function to each row and assign the result to the new column
    df['emi_external_id'] = df['ofvs'].apply(extract_emi_external_id)
    
    return df
