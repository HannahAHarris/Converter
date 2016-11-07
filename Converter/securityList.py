import pandas as pd
data = pd.read_csv('/Users/hharris/Desktop/SMARTS_output_2.csv',header =None)
print(data.head())
unique = data[0].drop_duplicates()
print(unique)
