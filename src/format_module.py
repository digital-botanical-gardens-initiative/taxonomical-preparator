'''Module to format inaturalist data'''



'''Function to create new formated location column'''
def location_formatting(df,old_column,new_column):
    #initialize the new column with NAs
    df[new_column] = 'NA'

    #loop over each line of the dataframe and change the new column value with the swiped values of the old column
    for i in range(len(df)):
        lat,lon = df[old_column][i]
        new_location = (lon,lat)
        df.at[i,new_column] = new_location

    #return the dataframe
    return df


'''Function to extract the dbgi id from the ofvs column and put it in a new column'''
def dbgi_id_extract(df):
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
                df.at[i,'emi_external_id'] = dct['value_ci']
