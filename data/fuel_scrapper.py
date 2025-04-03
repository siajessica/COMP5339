import os
import pandas as pd
import numpy
import csv
import requests
from bs4 import BeautifulSoup
import re
## openpyxl-3.1.5

def crawl(url = "https://data.nsw.gov.au/data/dataset/fuel-check"):
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
