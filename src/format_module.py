'''Module to format inaturalist data'''


'''Function to create new formated location column'''
def location_formatting(df,old_column,new_column):
    df[new_column] = 'NA'
    for i in range(len(df)):
        lat,lon = df[old_column][i]
        new_location = (lon,lat)
        df.at[i,new_column] = new_location

    return df


