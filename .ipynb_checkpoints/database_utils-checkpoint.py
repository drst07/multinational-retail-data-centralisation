import yaml

class DatabaseConnector:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as stream:
            data_loaded = yaml.safe_load(stream)
        print(type(data_loaded))
        return data_loaded

    def init_db_engine(self):
        creds = this.read_db_creds()
        print(creds)

dbObj = DatabaseConnector()
dbObj.init_db_engine()        
