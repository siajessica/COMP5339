import os
import pandas as pd
import numpy
import csv
import requests
from bs4 import BeautifulSoup
import re
import logging
import json
    
def crawler(url , params = None, headers = None, to_json = False):
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  
        if to_json:
            response = response.json()
        return response
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def save(dataframes, save_dir='data/', file_name = "all.csv"):
    '''
    Saves the provided pandas DataFrame into CSV file, tab-separated
    Args:
        dataframe (pandas.DataFrame)    : Dataframe to be saved
        location(str, optional)         : Specified target path and filename, defaults to current directory with filename as ./A  
    '''
    print("Combining it... ")
    os.makedirs(save_dir, exist_ok=True)
    
    location = os.path.join(save_dir, file_name)
    dataframes.to_csv(location, sep='\t', encoding='utf-8', index=False, header=True)
    print("saved at current directory")
    print("Complete!")

def convert_link_to_df(download_links):
    '''
    Download and combines data from provided list of URLs from both .CSV and .XLSX into a single DataFrame
    Args:
        download_links (list): A list that contains all URLs from crawl funtion pointing to valid .CSV and .XLSX files

    Returns:
        pandas.DataFrame: a single combined data frame from all downloaded files. Return error if the url is not valid. 
    '''
    dataframes = []
    print("I'm fetchin the data mate, hold yer horses... ")
    for url in download_links:
        if url.endswith(".xlsx"):
            df = pd.read_excel(url, engine="openpyxl")  # Read Excel files
        elif url.endswith(".csv"):
            df = pd.read_csv(url)  # Read CSV files
        else:
            continue  # Skip unsupported files
        dataframes.append(df)
    
    final_df = pd.concat(dataframes, ignore_index=True)
    return(final_df)