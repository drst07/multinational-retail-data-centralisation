from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Create a DatabaseConnector instance
db_connector = DatabaseConnector('db_creds.yaml')
engine = db_connector.init_db_engine()

# Create a DataExtractor instance
data_extractor = DataExtractor(db_connector)

# List all tables in the database
tables = db_connector.list_db_tables(engine)
print("Tables in the database:")
for table in tables:
    print(table)

# # Example: Read data from a table
# table_name = "legacy_users"  # Replace with the actual table name
# data = data_extractor.read_rds_table(table_name,engine)
# if data is not None:
#     print(f"Data from {table_name}:")
#     print(data)

# # Now, you can proceed to perform data cleaning, transformation, and uploading as needed.
# # Create a DataCleaning instance
data_cleaning = DataCleaning(db_connector)
# cleaned_user_data = data_cleaning.clean_user_data(data)

# # db_connector.upload_to_db(cleaned_user_data, 'dim_users')

# print(data_extractor.list_number_of_stores())
# st_data = data_extractor.retrieve_stores_data()
# print(st_data.head())

# # cleaned_store_data = data_cleaning.clean_store_data(st_data)
# # db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')

# s3_address = 's3://data-handling-public/products.csv'
# product_data = data_extractor.extract_from_s3(s3_address)
        
# cleaned_product_df = data_cleaning.convert_product_weights(product_data)

# cleaned_product_data = data_cleaning.clean_products_data(cleaned_product_df)
# db_connector.upload_to_db(cleaned_product_data, 'dim_products')

# read orders_table
table_name = "orders_table"  # Replace with the actual table name
data = data_extractor.read_rds_table(table_name,engine)
if data is not None:
    print(f"Data from {table_name}:")
    print(data)

# clean_order_data = data_cleaning.clean_orders_data(table_name,db_connector)

# db_connector.upload_to_db(clean_order_data, 'orders_table')


events_data = data_extractor.extract_from_s3_events()
cleaned_events_data = data_cleaning.clean_events(events_data)

db_connector.upload_to_db(cleaned_events_data, 'dim_date_times')

if __name__ == "__main__":
    