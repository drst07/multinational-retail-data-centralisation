import pandas as pd
from dateutil.parser import parse
import numpy as np
from data_extraction import DataExtractor

class DataCleaning:
    def __init__(self, db_connector):
        self.db_connector = db_connector
    def custom_date_parser(self, date_str):
        try:
            # Attempt to parse the date
            return parse(date_str)
        except:
            # Return a placeholder for non-date strings
            return np.datetime64('NaT')
    def clean_user_data(self,df):
        # df.to_csv('retaildata.csv')
        cleaned_df = df.copy()  # Create a copy to avoid modifying the original DataFrame
        cleaned_df = cleaned_df.drop_duplicates()
        # print(cleaned_df.info())
        ##check data types and fix
        #check for columns that should be string
        cleaned_df.first_name = cleaned_df.first_name.astype('string')
        cleaned_df.last_name = cleaned_df.last_name.astype('string')
        cleaned_df.company= cleaned_df.company.astype('string')
        cleaned_df.email_address= cleaned_df.email_address.astype('string')
        cleaned_df.address= cleaned_df.address.astype('string')
        cleaned_df.country= cleaned_df.country.astype('string')
        cleaned_df.country_code= cleaned_df.country_code.astype('string')
        cleaned_df['country_code'] = cleaned_df['country_code'].replace('GGB','GB')
        cleaned_df.phone_number= cleaned_df.phone_number.astype('string')
        cleaned_df.user_uuid= cleaned_df.user_uuid.astype('string')

        #check for columns that should be date
        cleaned_df['date_of_birth'] = cleaned_df['date_of_birth'].apply(self.custom_date_parser)
        cleaned_df['date_of_birth'] = pd.to_datetime(cleaned_df['date_of_birth'], infer_datetime_format=True, errors='coerce')
        cleaned_df['join_date'] = cleaned_df['join_date'].apply(self.custom_date_parser)
        cleaned_df['join_date'] = pd.to_datetime(cleaned_df['join_date'], infer_datetime_format=True, errors='coerce')
        
        # print(cleaned_df.iloc[1046:1048,:])
        # print("Total number of invalid date_of_birth value: ",cleaned_df['date_of_birth'].isna().sum())
        # print("Total number of invalid join_date value: ",cleaned_df['join_date'].isna().sum())

        #drop rows with invalid values for date columns
        cleaned_df = cleaned_df.dropna(subset=['join_date','date_of_birth'])

        cleaned_df.to_csv('cleaned_df.csv')
        # print(cleaned_df.shape)
     
        return cleaned_df
    
    def clean_card_number(self,card_number):
        return ''.join(char for char in str(card_number) if char.isdigit() or char != '?')

    def clean_card_data(self,df):
        # df.to_csv('card_orig.csv',index=False)
        pdf_dataframe = df[df["card_number"] != "NULL"]
        pdf_dataframe['card_number'] = pdf_dataframe['card_number'].apply(self.clean_card_number)
        pdf_dataframe = pdf_dataframe[pdf_dataframe['card_number'] != '']
        # pdf_dataframe = pdf_dataframe[pdf_dataframe["card_number"] != "card_number"]
        pdf_dataframe = pdf_dataframe[pd.to_numeric(pdf_dataframe['card_number'], errors='coerce').notnull()]
        # pdf_dataframe.to_csv('card_data2.csv',index=False)
        print(pdf_dataframe .head())
        pdf_dataframe.to_csv('card3.csv',index=False)
        return pdf_dataframe
    
    def clean_store_data(self,df):
        cleaned_df = df.copy()  # Create a copy to avoid modifying the original DataFrame
        cleaned_df.to_csv('store_data_orig.csv')
        # cleaned_df = cleaned_df.drop_duplicates()
        # print(cleaned_df.shape)
        cleaned_df.replace({'continent': ['eeEurope', 'eeAmerica']}, {'continent': ['Europe', 'America']}, inplace=True)
        cleaned_df['opening_date'] = pd.to_datetime(cleaned_df['opening_date'], infer_datetime_format=True, errors='coerce')
        cleaned_df.drop(columns='lat', inplace=True)
        cleaned_df['staff_numbers'] = cleaned_df['staff_numbers'].str.replace(r'[a-zA-Z]', '', regex=True)
        cleaned_df['staff_numbers'] = pd.to_numeric(cleaned_df["staff_numbers"], errors='coerce')
        cleaned_df["longitude"] = pd.to_numeric(cleaned_df["longitude"], errors='coerce')
        cleaned_df["latitude"] = pd.to_numeric(cleaned_df["latitude"], errors='coerce')
        
        cleaned_df = cleaned_df.replace('N/A', np.nan)
        cleaned_df = cleaned_df.replace('NULL', np.nan)
        # cleaned_df.dropna(subset=['opening_date', 'store_type','staff_numbers'], inplace=True)
        cleaned_df.dropna(subset=['store_code'], inplace=True)
        # cleaned_df['longitude'] = cleaned_df['longitude'].astype(float)
        # cleaned_df['latitude'] = cleaned_df['latitude'].astype(float)
        # print(cleaned_df.info())
        # print(cleaned_df.shape)
        # cleaned_df.to_csv('store_data.csv')
        cleaned_df.to_csv('store_data2.csv')
        return cleaned_df

    def fix_weird_value(self, value):
        if isinstance(value, str) and 'x' in value:
            parts = value.split(' x ')
            try:
                num1 = float(parts[0])
                num2 = float(parts[1])
                return f"{(num1 * num2)}"
            except ValueError:
                pass
        else:
            return value

    def convert_product_weights(self,df):
        # df = data.copy()
        # df = df.drop_duplicates()
        # print(df.info())
        # Define a regular expression to extract the numeric part and the unit part
        df['weight'] = df['weight'].astype(str)
        regex_pattern = r'(?P<weightnumeric>\d+(\.\d+)?)\s*(?P<unit>[a-zA-Z]+)'
        df_extracted = df['weight'].str.extract(regex_pattern)

        # Drop the decimal part column if it exists
        df_extracted.drop(1, axis=1, inplace=True, errors='ignore')

        # Concatenate the extracted columns with the original DataFrame
        df_result = pd.concat([df, df_extracted], axis=1)
        # print(df_result.head(10))

        df_result['weightnumeric'] = pd.to_numeric(df_result['weightnumeric'], errors='coerce')  # Convert to numeric, handle non-numeric values as NaN
        df_result['weightnumeric'] = df_result['weightnumeric'].apply(self.fix_weird_value)
        
        # Update 'weightnumeric' column based on conditions
        df_result['weight'] = df_result.apply(lambda x: x['weightnumeric']/1000 if x['unit']=='g' or x['unit']=='ml' else x['weightnumeric'], axis=1)
        df_result.drop(columns=['weightnumeric','unit'], inplace=True)
        # print('aa: ',df_result['weight'].head(10))
        return df_result
    
    def clean_products_data(self,products_dataframe):
        # products_dataframe = products_dataframe.drop_duplicates()
        products_df=products_dataframe
        # products_df=self.convert_product_weights(products_dataframe)#sets the rpoducts dataframe from the cleaned weights dataframe in the previous method
        # products_df.replace('', np.nan, inplace=True)
        products_df.dropna(subset=['uuid', 'product_code','removed'], inplace=True)
        products_df['date_added']=pd.to_datetime(products_df['date_added'], format='%Y-%m-%d', errors='coerce')
        drop_prod_list=['S1YB74MLMJ','C3NCA2CL35', 'WVPMHZP59U']# list of strings to drop rows for in the next line
        products_df.drop(products_df[products_df['category'].isin(drop_prod_list)].index, inplace=True)# drop the rows where the category column has entries equal to thouse in the list above
        # print(products_df.shape)
        # print('bb: ',products_df['weight'].head())
        return products_df
    
    def clean_orders_data(self,table_name,db_connector):
        print(table_name)
        data_extractor = DataExtractor(db_connector)
        engine = db_connector.init_db_engine()
        df = data_extractor.read_rds_table(table_name, engine)
        # print(df.head())
        # df = df.drop_duplicates()
        df.drop(columns=['first_name','last_name','1','level_0'],inplace=True)
        print(df.head())
        df.to_csv('cleaned_orders.csv',index=False)
        return df
    
    def clean_events(self,df):
        date_time_dataframe = df.copy()
        print(date_time_dataframe.shape)
        date_time_dataframe['day'] = pd.to_numeric(date_time_dataframe['day'], errors='coerce')
        date_time_dataframe.dropna(subset=['day', 'year', 'month'], inplace=True)
        date_time_dataframe['timestamp'] = pd.to_datetime(date_time_dataframe['timestamp'], format='%H:%M:%S', errors='coerce')# timestamp in form hour minute and seconds
        print(date_time_dataframe.shape)
        date_time_dataframe.to_csv('date_time_dataframe_cleaned.csv',index=False)
        return date_time_dataframe

