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
    import numpy as np

    df['emi_external_id'] = np.nan
    
    #flags = re.IGNORECASE
    df.ofvs = df.ofvs.astype(str)
    #for i in range(len(df)):
     #   if re.findall("(?<='value_ci': ')(.{1,30})(?=', 'name': 'emi_external_id')",df.ofvs[i],flags):
      #      df.at[i,'dbgi_id'] = re.findall("(?<='value_ci': ')(.{1,30})(?=', 'name': 'emi_external_id')",df.ofvs[i],flags)[0].lower()
    for i in range(len(df)):
        dct = dict()
        if df.ofvs[i] != '[]':
            ls = df.ofvs[i].replace(' ','').replace('\'','')  
            for j in ls.split(','):
                j = j.split(':')
                dct[j[0]] = j[1]
            if dct['name_ci'] == 'emi_external_id':
                df.at[i,'emi_external_id'] = dct['value_ci']
