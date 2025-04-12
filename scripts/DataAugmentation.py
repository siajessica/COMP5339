import os
import pandas as pd
import numpy
import csv
import requests
import re  
import logging
import json

from datetime import datetime
from bs4 import BeautifulSoup
from helper.crawler import crawler, save, convert_link_to_df

class DataAugmentation():
    def __init__(self):
        """
        Initiation for Data Augmentation, augmenting multiple sources from different APIs
        """
        creds = self.__read_creds()
        self.API_KEY_FUEL = creds["API_KEY_FUEL"]
        self.API_SECRET_FUEL = creds["API_SECRET_FUEL"]
        self.AUTH_FUEL = creds["AUTH_FUEL"]
        self.API_BRANDFETCH = creds["API_BRANDFETCH"]
        
    def __read_creds(self, creds_path = "creds/creds.json"):
        """
        Reading the credentials to access the APIs
        Args:
            creds_path (str, optional):   The path where credentials are stored 
        Returns:
            list:    Dictionary containing all the API credentials
        """
        with open("creds/creds.json") as file:
            data = json.load(file)
        return data
        
    def FuelCheckIntegration(self, url = "https://data.nsw.gov.au/data/dataset/fuel-check"):
        """
        Locate and extract download links for CSV and XLSX files that appear from the url from year 2024 to 2025
        Args:
            url (str, optional):    Starting url, 
                                    default the NSW web page, 
                                    basically this code will not work on different url. 
        Returns:
                list:    A list of string that contains the url to download the file
        """
        
        response = crawler(url)
        soup = BeautifulSoup(response.content, "html.parser")
        download_links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href and (href.endswith(".csv") or href.endswith(".xlsx")) and ("m24." in href or "mar25." in href):
                download_links.append(href)
        if download_links:
            #print("Download links found (2024 or 2025):")
            for link in download_links:
                print('found!')
        else:
            print("No CSV or XLSX download links containing 2024 or 2025 found on the page.")

        print(str(len(download_links)) + ' download link(s) found')
        
        df = convert_link_to_df(download_links)
        return df

    def OpenStreetMapAPI(self, query):
        """
        Getting the Lattitude and Longitude using the OpenStreetMapAPI
        Args:
                query: the cities or location we would like to get its latitude and longitude
        Returns:
                tuple: (latitude, longitude)
        """
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': query,
            'format': 'json'
        }
        headers = {
            'User-Agent': 'FuelCheck'
        }
        data = crawler(url, headers=headers, params=params, to_json=True)
        if data:
            return data[0]['lat'], data[0]['lon']
        else:
            print("No Latitude Longitude Found")
            return None, None
        
    def GetFuelAccessToken(self):
        """
        Getting the accesstoken from the Fuel API security https://api.nsw.gov.au/Documentation/GenerateHar/22
        Returns:
            tuple: access token generated from the Fuel API security
        """
        
        GRANT_TYPE = "client_credentials"
        URL_SECURITY = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
        
        headers = {
            'content-type': "application/json",
            'authorization': self.AUTH_FUEL
        }

        querystring = {"grant_type":GRANT_TYPE}
        response = crawler(URL_SECURITY, params=querystring, headers=headers, to_json=True)
        return response["access_token"]

    def FuelStationIntegration(self):
        """
        Getting the Lattitude and Longitude from the Fuel API security https://api.nsw.gov.au/Documentation/GenerateHar/22
        Returns:
            DataFrame: containing the station details
        """
        
        url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices"
        access_token = self.GetFuelAccessToken()
        
        headers = {
            'content-type': "application/json; charset=utf-8",
            'authorization': f"Bearer {access_token}",
            'apikey': self.API_KEY_FUEL,
            'transactionid': "1234567890",
            'requesttimestamp': datetime.utcnow().strftime('%d/%m/%Y %I:%M:%S %p')
        }
        resp = crawler(url=url, headers=headers, to_json=True)
        stations = resp.get("stations", []) 
        df_station_api = pd.json_normalize(stations) 
        return df_station_api

    def BrandFetchAPI(self, company, root_save_dir = "src/", save =True):
        """
        This is used to call API to fetch the logos of the brand
        Args:
            company: the name of the company we want to get the logo
            root_save_dir: the directory where we save the logos
            save: (bool) whether or not we save the logo or just use the API's image link
        Returns:
            response: the image link or local path
        """
        base_url = "https://api.brandfetch.io/v2/search/"

        header = {
            "Authorization": f"Bearer {self.API_BRANDFETCH}"
        }

        response = crawler(url=base_url+company, headers=header, to_json=True)
        if(len(response) == 0):
            print("No Logo Found for ", company)
            return 
        
        img_link = response[0]["icon"]
        if save:
            response = crawler(img_link)
            save_dir = os.path.join(root_save_dir, company+".jpg")
            with open(save_dir, "wb") as f:
                f.write(response.content)
            
            img_link = save_dir
        
        return img_link
    
    def BrandLogoIntegration(self, company_lst, root_save_dir = "./src/", save= True):
        """
        This is used to integrate the logos and create the Dataframe
        Args:
            company: the name of the company we want to get the logo
            root_save_dir: the directory where we save the logos
            save: (bool) whether or not we save the logo or just use the API's image link
        Returns:
            response: the image link or local path
        """
        company_data = []
        os.makedirs(root_save_dir, exist_ok=True)
        for company in company_lst:
            company_path = os.path.join(root_save_dir, company + ".jpg")
            if save and not os.path.exists(company_path):
                self.BrandFetchAPI(company=company, root_save_dir=root_save_dir, save=True)
            elif not save:
                company_path = self.BrandFetchAPI(company=company, root_save_dir=root_save_dir, save=False)
            
            company_dict = {
                "brand": company,
                "img_path": company_path
            }
            company_data.append(company_dict)
            
        df_brand = pd.DataFrame(company_data)
        return df_brand
            
    
    def DataAugmented(self, combine_all = False, deep_search = True, save_dir = "data/", fuel_name = "fuel_price.csv", station_name = "station_detail.csv", brand_name = "brand.csv"):
        """
        This is used to combine all the sources into multiple Dataframes to be processed further to the database
        Args:
            company: the name of the company we want to get the logo
            combine_all: If True we will generate the combined dataframes of prices and station details
            deep_search: If True we will find the lattitude and longitude when it does not exist in the Fuel API
            save_dir: is the directory to save the csv file
            fuel_name: The name of the fuel csv
            station_name: The name of the station detail csv
            brand_name: The name of brand csv
        Returns:
            response: the image link or local path
        """
        df_fuel_price = self.FuelCheckIntegration()
        df_station_details = self.FuelStationIntegration()
        company_lst = list(df_station_details["brand"].unique())
        df_brand = self.BrandLogoIntegration(company_lst)
        
        save(df_fuel_price, save_dir, file_name=fuel_name)
        save(df_station_details, save_dir, file_name=station_name)
        save(df_brand, save_dir, file_name=brand_name)
        
        if combine_all:
            df_merged = df_fuel_price.merge(
                            df_station_details[["location.latitude", "location.longitude", "isAdBlueAvailable", "name"]],
                            how="left", 
                            left_on="ServiceStationName", 
                            right_on="name" 
                        )
            df_merged.drop("name",axis=1, inplace=True)
            
            if deep_search:
                missing_address = df_merged[df_merged["location.latitude"].isna()]["Address"].unique()
                for miss_long_lat in missing_address:
                    long, lat = self.OpenStreetMapAPI(miss_long_lat)
                    
                    df_merged.loc[
                        df_merged["Address"] == miss_long_lat,
                        "location.latitude"
                    ] = lat
                    
                    df_merged.loc[
                        df_merged["Address"] == miss_long_lat,
                        "location.longitude"
                    ] = long
                
                
            save(df_merged)