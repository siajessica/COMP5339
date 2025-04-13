from scripts.DataAugmentation import DataAugmentation

"""
This command will generate all the logos the following:
1. All logos inside src folders
2. All Dataframe datas inside data folder 

You can analyze these dataframes to be used for the next part which is the dataset
"""

da = DataAugmentation()
da.DataAugmented(combine_all=True, deep_search=True)