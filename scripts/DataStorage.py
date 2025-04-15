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
                CREATE TABLE brand AS
                SELECT 
                    row_number() OVER () AS brand_id,
                    * 
                FROM read_csv('{file_path}', header = True, auto_detect = True)
            """)

            #add integrity constraint for primary key
            self.conn.execute(f"""
                ALTER TABLE brand
                ADD CONSTRAINT pk_brand PRIMARY KEY (brand_id)
            """)
            # self.conn.sql("SELECT * FROM BRAND LIMIT 10").show()
        elif file_path == "data/all.csv":
            self.conn.execute(f"""
                CREATE TABLE augmented_data AS
                SELECT * 
                FROM read_csv('{file_path}', header = True, auto_detect = True)
            """)
            # self.conn.sql("SELECT * FROM augmented_data LIMIT 10").show()

    def create_location_table(self):
        """
        LocationID,Adress,location.latitude,location.longitude
        """
        self.conn.execute(f"""
            CREATE TABLE location AS
            SELECT 
                row_number() OVER () AS location_id,
                address,
                latitude,
                longitude
            FROM (
                SELECT DISTINCT 
                    Address AS address,
                    "location.latitude" AS latitude,
                    "location.longitude" AS longitude
                FROM augmented_data
            )
        """)
        self.conn.execute(f"""
            ALTER TABLE location
            ADD CONSTRAINT pk_location PRIMARY KEY (location_id)
        """)
        # self.conn.sql("SELECT * FROM Location LIMIT 10").show()

    def create_service_station_table(self):
        """
        ServiceStationID(PK),ServiceStationName,LocationID(FK),BrandID(FK),isAdBlueAvailable
        """
        self.conn.execute(f"""
            CREATE TABLE service_station (
                station_id BIGINT PRIMARY KEY,
                service_station_name VARCHAR,
                is_ad_blue_available BOOL,
                brand_id BIGINT,
                location_id BIGINT,
                --CONSTRAINT fk_brand FOREIGN KEY (brand_id) REFERENCES brand(brand_id),
                --CONSTRAINT fk_location FOREIGN KEY (location_id) REFERENCES location(location_id)
            )
        """)
        self.conn.execute(f"""
            INSERT INTO service_station (station_id, service_station_name, is_ad_blue_available, brand_id, location_id)
            SELECT 
                row_number() over () as station_id,
                ServiceStationName as service_station_name,
                isAdBlueAvailable as is_ad_blue_available,
                brand_id,
                location_id
            FROM (
                SELECT DISTINCT
                    ServiceStationName,
                    isAdBlueAvailable,
                    brand_id,
                    location_id
                FROM 
                    augmented_data ad
                INNER JOIN 
                    brand b
                ON
                    ad.Brand = b.brand
                INNER JOIN
                    location l
                ON
                    l.address = ad.Address AND
                    l.longitude = ad."location.longitude" AND
                    l.latitude = ad."location.latitude"
            )
        """)
        # self.conn.sql("SELECT * FROM service_station LIMIT 15").show()

    def create_fuel_price_table(self):
        """
        ServiceStationID(FK),FuelCode,PriceUpdatedDate,Price
        Remember to Partition based on FuelCode, Daily/Weekly
        """  
        self.conn.execute(f"""
            CREATE TABLE fuel_price (
                station_id BIGINT,
                fuel_code VARCHAR,
                price_updated_date DATETIME,
                price FLOAT,
                --CONSTRAINT fk_station FOREIGN KEY (station_id) REFERENCES service_station(station_id),
                --CONSTRAINT pk_fuel_price PRIMARY KEY (station_id, fuel_code, price_updated_date)
            )
        """)
        self.conn.execute(f"""
            INSERT INTO fuel_price (station_id, fuel_code, price_updated_date, price)
            SELECT DISTINCT
                s.station_id,
                ad.FuelCode AS fuel_code,
                ad.PriceUpdatedDate AS price_updated_date,
                ad.Price AS price
            FROM
                augmented_data ad
            INNER JOIN
                service_station s
            ON
                s.service_station_name = ad.ServiceStationName
        """)
        # self.conn.execute(f"""
        #     ALTER TABLE fuel_price
        #     ADD CONSTRAINT pk_fuel_price PRIMARY KEY (station_id, fuel_code, price_updated_date);
        # """)
        # self.conn.sql("SELECT * FROM fuel_price LIMIT 15").show()

    def drop_all_tables(self):
        tables = ["fuel_price", "service_station", "location", "brand", "augmented_data"]
        for table in tables:
            self.conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")


    def load_db(self):
        """
        load_db() loads the cleaned data from the csv files and separates them into the tables required by the schema.
        Use drop_all_tables() in order to drop all tables created in this class.
        """
        self.drop_all_tables()
        self.load_data("data/brand.csv")
        self.load_data("data/all.csv")
        self.create_location_table()
        self.create_service_station_table()
        self.create_fuel_price_table()