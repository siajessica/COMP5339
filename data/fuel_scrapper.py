import os
import pandas as pd
import numpy
import csv
import requests
from bs4 import BeautifulSoup
import re
## openpyxl-3.1.5

def crawl(url = "https://data.nsw.gov.au/data/dataset/fuel-check"):
    """
    Locate and extract download links for CSV and XLSX files that appear from the url from year 2024 to 2025
    Args:
        url (str, optional):    Starting url, 
                                default the NSW web page, 
                                basically this code will not work on different url. 
    Returns:
            list:    A list of string that contains the url to download the file
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  
        soup = BeautifulSoup(response.content, "html.parser")
        download_links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href and (href.endswith(".csv") or href.endswith(".xlsx")) and ("m24." in href or "mar25." in href):
                download_links.append(href)
        if download_links:
            #print("Download links found (2024 or 2025):")
            for link in download_links:
                #print(link)
                print('found!')
        else:
            print("No CSV or XLSX download links containing 2024 or 2025 found on the page.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    print(str(len(download_links)) + ' download link(s) found')
    return download_links

def combine(download_links):
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

def save(dataframes, location='./All.csv'):
    '''
    Saves the provided pandas DataFrame into CSV file, tab-separated
    Args:
        dataframe (pandas.DataFrame)    : Dataframe to be saved
        location(str, optional)         : Specified target path and filename, defaults to current directory with filename as ./A  
    '''
    print("Combining it... ")
    dataframes.to_csv(location, sep='\t', encoding='utf-8', index=False, header=True)
    print("saved at current directory")
    print("Complete!")

if  __name__ == '__main__':
    #user_input = input("Enter your path (Example: Z:/Desk/USyd/COMP5339.csv): ")
    current_dir = os.getcwd()
    file_name = "all.csv"
    # Create the full path
    loc = os.path.join(current_dir, file_name)
    loc = os.path.normpath(loc)  
    save(combine(crawl()),loc)
