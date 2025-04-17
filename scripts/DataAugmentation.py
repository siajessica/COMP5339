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
        self.GOOGLE_MAP_API = creds["GOOGLE_MAP_API"]
        
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
            if href and (href.endswith(".csv") or href.endswith(".xlsx")) and ("24." in href or "25." in href):
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

    def GoogleMapAPI(self, query):
        # Endpoint
        url = "https://places.googleapis.com/v1/places:searchText"

        # Headers
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.GOOGLE_MAP_API,
            "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.location,places.regularOpeningHours"
        }

        # Request body
        data = {
            "textQuery": query,
        }

        try:
            # Make the POST request
            response = requests.post(url, headers=headers, json=data)
            places = response.json().get("places", [])
            return places
        except Exception as e:
            print("ERROR FETCHING GOOGLE MAP API: ", e)
            print(response.text)
            return None

    def clean_opening_hour(self, entry):
        # Remove any unicode narrow no-break space or thin space
        cleaned = re.sub(r'[\u202f\u2009]', ' ', entry)
        # Split into day and hour
        weekday, hours = cleaned.split(': ', 1)
        return weekday, hours

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
    
    def DataCleaning(self, df):
        """
        This is the step to clean the data. 
        The data has some duplicated data that needs to be cleaned
        Args:
            df: Dataframe to be cleaned
        Returns:
            df: the cleaned Dataframe
        """
        
        # Removing Duplicates
        df = df.groupby(['ServiceStationName', 'Address','FuelCode', 'PriceUpdatedDate'], as_index=False).agg({
            'Suburb': 'first',
            'Postcode': 'first',
            'Brand': 'first',
            'Price': 'mean'
        })

        # Convert PriceUpdateDate to Timestamp
        df["PriceUpdatedDate"] = pd.to_datetime(df['PriceUpdatedDate'])
        return df

    def CombiningStationDetails(self, df_fuel):
        station_dict = {}
        opening_dict = []
        
        print("Combining Data from Google API")

        ## Getting all the unique station
        unique_stations = list(df_fuel[["ServiceStationName"]].drop_duplicates()["ServiceStationName"])
        df_location = df_fuel.copy()

        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        for station in unique_stations:
            resp = self.GoogleMapAPI(station)

            if resp is None or len(resp) == 0:
                print(f"Station Not Found {station}")
                continue

            resp = resp[0]
            station_dict[station] = {
                "longitude": resp["location"]["longitude"],
                "latitude": resp["location"]["latitude"]
            }
            try:
                for idx, p in enumerate(resp["regularOpeningHours"]["periods"]):
                    if "close" in p.keys():
                        opening_dict.append({
                            "ServiceStationName": station,
                            "day_of_week": days[p["open"]["day"]],
                            "open_time": f'{p["open"]["hour"]:02d}:{p["open"]["minute"]:02d}',
                            "close_time": f'{p["close"]["hour"]:02d}:{p["close"]["minute"]:02d}'
                        })
                    else:
                        if "regularOpeningHours" in resp.keys():
                            for day in resp["regularOpeningHours"]["weekdayDescriptions"]:
                                opening_dict.append({
                                    "ServiceStationName": station,
                                    "day_of_week": re.sub(r':.*$', '', day).strip(),
                                    "open_time": "00:00",
                                    "close_time": "24:00"
                                })
                        else:
                            print(f"Station has no opening hours: {station}")
                            continue
            except Exception as e:
                print(f"Station has no opening hours: {station}")
                continue
        
        df_opening_hours = pd.DataFrame(opening_dict)

        df_location["latitude"] = df_location["ServiceStationName"].map(
            lambda x: station_dict.get(x, {}).get("latitude", df_location.loc[df_location["ServiceStationName"] == x, "latitude"].values[0])
        )
        df_location["longitude"] = df_location["ServiceStationName"].map(
            lambda x: station_dict.get(x, {}).get("longitude", df_location.loc[df_location["ServiceStationName"] == x, "longitude"].values[0])
        )
        df_location = df_location[["Address", "latitude", "longitude"]]

        return df_opening_hours, df_location

    def __extract_to_parquet(self, df, table_name, partition = None, save_dir = "./data"):
        df.to_parquet(
            f'{save_dir}/{table_name}',
            partition_cols=partition,
            engine='pyarrow',
            index=False
        )

    def DataAugmented(self, combine_all = False, deep_search = True, save_dir = "data/", fuel_name = "fuel_price.csv", station_name = "station_detail.csv", brand_name = "brand.csv", cleaning = True):
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
        print("Cleaning Data")
        df_fuel_price = self.DataCleaning(df_fuel_price)     

        print("Getting Data from Fuel Price API for Ad Blue")
        df_station_details = self.FuelStationIntegration()

        company_lst = list(df_station_details["brand"].unique())
        df_brand = self.BrandLogoIntegration(company_lst)

        # Putting the isAdBlue Field inside the main table
        print("Combining Fuel Price and the External API")
        df_merged = df_fuel_price.merge(
                        df_station_details[["location.latitude", "location.longitude","isAdBlueAvailable", "name"]],
                        how="left", 
                        left_on="ServiceStationName", 
                        right_on="name" 
                    )
        df_merged.drop("name",axis=1, inplace=True)

        df_merged.rename(columns={'Brand': 'brand_name'}, inplace=True)
        df_merged.rename(columns={'location.latitude': 'latitude'}, inplace=True)
        df_merged.rename(columns={'location.longitude': 'longitude'}, inplace=True)
        df_merged.rename(columns={'isAdBlueAvailable': 'is_ad_blue_available'}, inplace=True)
        df_brand.rename(columns={'Brand': 'brand_name'}, inplace=True)
        
        # Creating table to be extract to parquet
        fact_table = df_merged[["ServiceStationName", "FuelCode", "PriceUpdatedDate", "Price"]]
        station_detail_table = df_merged[["ServiceStationName", "Address", "brand_name", "is_ad_blue_available", "latitude", "longitude"]]

        print("Getting data from Google Map API")
        opening_hours_table, location_table = self.CombiningStationDetails(station_detail_table)

        station_detail_table = station_detail_table["ServiceStationName", "Address", "brand_name", "is_ad_blue_available"]

        # Save to csv for debuging purpose
        save(fact_table, save_dir, file_name=fuel_name)
        save(station_detail_table, save_dir, file_name=station_name)
        save(df_brand, save_dir, file_name=brand_name)
        save(opening_hours_table, save_dir, file_name="opening_hours.csv")
        save(location_table, save_dir, file_name="location.csv")

        #Extract to Parquet Files
        print("Extracting data to parquet")
        self.__extract_to_parquet(fact_table, "fuel_price", partition=["FuelCode"])
        self.__extract_to_parquet(station_detail_table, "service_station")
        self.__extract_to_parquet(opening_hours_table, "opening_hours")
        self.__extract_to_parquet(location_table, "location")
        self.__extract_to_parquet(df_brand, "brand")