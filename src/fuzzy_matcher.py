# We test fuzzy matching among dataframes
# https://www.geeksforgeeks.org/how-to-do-fuzzy-matching-on-pandas-dataframe-column-using-python/


import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
  
# creating the dictionaries
dict1 = {'name': ["mango", "coco", "choco", "peanut", "apple"]}
dict2 = {'name': ["mango fruit", "coconut", "chocolate",
                  "mangoes", "chocos", "peanuts", "appl"]}
  
# converting to pandas dataframes
dframe1 = pd.DataFrame(dict1)
dframe2 = pd.DataFrame(dict2)
  
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
list1 = dframe1['name'].tolist()
list2 = dframe2['name'].tolist()
  
# taking the threshold as 82
threshold = 82
  
# iterating through list1 to extract 
# it's closest match from list2
for i in list1:
    mat1.append(process.extract(i, list2, limit=2))
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



