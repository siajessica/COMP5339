import duckdb
import pandas as pd
import os

class DataStorage:
    def __init__(self, db_path):
        self.conn = duckdb.connect(db_path)
    
    def load_data(self, file_path):
        """
        Load specified csv into db
        """
        if file_path == "data/brand.csv":
            #duckdb doesn't have auto-increment, we have to use row_number as a replacement
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE Brand AS
                SELECT 
                    row_number() over () as brand_id,
                    * 
                FROM read_csv('{file_path}', header = True, auto_detect = True)
            """)

            #add integrity constraint for primary key
            self.conn.execute(f"""
                ALTER TABLE Brand
                ADD CONSTRAINT pk_brand PRIMARY KEY (brand_id)
            """)
            # self.conn.sql("SELECT * FROM BRAND LIMIT 10").show()
        elif file_path == "data/all.csv":
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE AugmentedData AS
                SELECT * 
                FROM read_csv('{file_path}', header = True, auto_detect = True)
            """)
            # self.conn.sql("SELECT * FROM AugmentedData LIMIT 10").show()

    def create_location_table(self):
        """
        LocationID,Adress,location.latitude,location.longitude
        """
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE Location AS
            SELECT 
                row_number() over () as location_id,
                address,
                latitude,
                longitude
            FROM (
                SELECT DISTINCT 
                    Address as address,
                    "location.latitude" as latitude,
                    "location.longitude" as longitude
                FROM AugmentedData
            )
        """)
        self.conn.execute(f"""
            ALTER TABLE Location
            ADD CONSTRAINT pk_location PRIMARY KEY (location_id)
        """)
        # self.conn.sql("SELECT * FROM Location LIMIT 10").show()

    def create_service_station_table(self):
        """
        ServiceStationID(PK),ServiceStationName,LocationID(FK),BrandID(FK),isAdBlueAvailable
        """
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE ServiceStation AS
            SELECT 
                row_number() over () as station_id,
                ServiceStationName,
                isAdBlueAvailable,
                brand_id,
                location_id
            FROM (
                SELECT DISTINCT
                    ServiceStationName,
                    isAdBlueAvailable,
                    brand_id,
                    location_id
                FROM 
                    AugmentedData ad
                INNER JOIN 
                    Brand b
                ON
                    ad.Brand = b.brand
                INNER JOIN
                    Location l
                ON
                    l.address = ad.Address AND
                    l.longitude = ad."location.longitude" AND
                    l.latitude = ad."location.latitude"
            )
        """)

        self.conn.execute(f"""
            ALTER TABLE ServiceStation
            ADD CONSTRAINT pk_service_station PRIMARY KEY (station_id);
        """)
        # self.conn.sql("SELECT * FROM ServiceStation LIMIT 15").show()

    def create_fuel_price_table(self):
        """
        ServiceStationID(FK),FuelCode,PriceUpdatedDate,Price
        Remember to Partition based on FuelCode, Daily/Weekly
        """
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE FuelPrice AS 
            SELECT DISTINCT
                s.station_id,
                ad.FuelCode,
                ad.PriceUpdatedDate,
                ad.Price
            FROM
                AugmentedData ad
            INNER JOIN
                ServiceStation s
            ON
                s.ServiceStationName = ad.ServiceStationName
        """)
        # self.conn.execute(f"""
        #     ALTER TABLE FuelPrice
        #     ADD CONSTRAINT pk_fuel_price PRIMARY KEY (station_id, FuelCode, PriceUpdatedDate);
        # """)
        # self.conn.sql("SELECT * FROM FuelPrice LIMIT 15").show()

    def load_db(self):
        # loading data into db before transformation:
        self.load_data("data/brand.csv")
        self.load_data("data/all.csv")
        self.create_location_table()
        self.create_service_station_table()
        self.create_fuel_price_table()