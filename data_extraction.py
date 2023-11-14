
from sqlalchemy import inspect
from sqlalchemy import text
from database_utils import DatabaseConnector
import pandas as pd
import requests
import boto3
from io import BytesIO
import tabula

class DataExtractor:
    ''' 
    This class will work as a utility class, in it you will be creating methods that 
    help extract data from different data sources.
    The methods contained will be fit to extract data from a particular data source, 
    these sources will include CSV files, an API and an S3 bucket.
    '''
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.api_key ={'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        

    def read_rds_table(self, table_name, engine):
        # if self.db_connector.engine is not None:
            try:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, con=engine)
                return df
            except Exception as e:
                print(f"Error reading data from {table_name}: {str(e)}")
                return None
        # else:
        #     print("Error: Database engine not initialized.")
        #     return None

    def list_number_of_stores(self):
        stores = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',headers=self.api_key)
        number_of_stores = stores.json()
        return number_of_stores["number_stores"]
    
    def retrieve_stores_data(self):
        num_of_stores = self.list_number_of_stores()
        print('num_of_stores:',num_of_stores)
        stores_data=[]
        try:
            for store_num in range(0,num_of_stores):
                store_endpoint = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_num}"
                response = requests.get(store_endpoint, headers=self.api_key)
                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                        store_data = response.json()
                        stores_data.append(store_data)
                        
                else:
                        print(f"Error: Unable to retrieve data for store {store_num}. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error: {e}")

        # Create a pandas DataFrame from the extracted store data
        if stores_data:
            df = pd.DataFrame(stores_data)
            print(df.head())
            print(df.shape)
            return df
        else:
            return None
        
    def retrieve_pdf_data(self,pdfurl):
        pdf_tables = tabula.read_pdf(pdfurl, pages='all', multiple_tables=True)
        pdf_data = pd.concat(pdf_tables, ignore_index=True)
        return pdf_data
        
    def extract_from_s3(self,s3_address):
        s3_client = boto3.client('s3')
        bucket, key = s3_address.replace('s3://', '').split('/', 1)
        print(bucket, key)
        # Download the file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        # Create a DataFrame from the downloaded data
        df = pd.read_csv(BytesIO(data))
        # print(df.head())
        # df.to_csv('products_data.csv')
        return df
    
    def extract_from_s3_events(self):
        #https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json
        s3 = boto3.client('s3')
        bucket = 'data-handling-public'
        object = 'date_details.json'
        file = 'date_details.json'
        s3.download_file(bucket,object,file)
        table = pd.read_json('./date_details.json')
        print(table.head())
        table.to_csv('events.csv')
        return table


