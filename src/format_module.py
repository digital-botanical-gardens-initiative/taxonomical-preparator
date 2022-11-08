'''Module to format inaturalist data'''



'''Function to create new formated location column'''
def location_formatting(df,old_column,new_column):
    df[new_column] = 'NA'
    for i in range(len(df)):
        lat,lon = df[old_column][i]
        new_location = (lon,lat)
        df.at[i,new_column] = new_location

    return df


'''Function to extract the dbgi id from the ofvs column and put it in a new column'''
def dbgi_id_extract(df):
    import re

    df['dbgi_id'] = ''
    flags = re.IGNORECASE
    df.ofvs = df.ofvs.astype(str)
    for i in range(len(df)):
        if re.findall(r'dbgi_spl\w+',df.ofvs[i],flags):
            df.at[i,'dbgi_id'] = re.findall(r'dbgi_spl\w+',df.ofvs[i],flags)[0]
