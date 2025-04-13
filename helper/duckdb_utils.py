import duckdb

"""
Create table augmented_data in duckdb for all.csv using read_csv
Create new tables in duckdb using schema
Insert data from augmented_data into separate tables using distinct keyword
"""

def create_augmented_table():
	"""
	Create augmented_data table using all.csv.
	"""
	duckdb.sql(f"""
		CREATE TABLE augmented_data AS
		SELECT * FROM read_csv('../data/all.csv', header=True, auto_detect=True)
	""")
