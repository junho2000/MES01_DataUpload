import pymysql
import pandas as pd
import os
from dotenv import load_dotenv

class AWSUploader():
    def __init__(self):
        load_dotenv()
        hostname = os.getenv('DB_HOSTNAME')
        username = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database = os.getenv('DB_NAME')

        self.connection = pymysql.connect(
            host=hostname,
            port=3306,
            user=username,
            passwd=password,
            db=database,
        )
        self.cursor = self.connection.cursor()
        self.is_mysql_connected(self.connection)
    
    def is_mysql_connected(self, connection):
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                print("success to connect to MySQL")
                return True
        except pymysql.MySQLError as e:
            print(f"Failed to connect to MySQL: {e}")
            return False
    
    def upload_csv(self, file_path):
        data = pd.read_csv(file_path, index_col=0)
        columns = ['Time', 'Throttle', 'Speed', 'Steering', 'Brake', 'Acc', 'LocationX', 'LocationY', 'Distance']
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO raw_data ({', '.join(columns)}) VALUES ({placeholders})"
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in data.to_numpy()]
        try:
            self.cursor.executemany(sql, data_tuples)
            self.connection.commit()
            print("All data uploaded successfully")
        except pymysql.MySQLError as e:
            print(f"Error during bulk insert: {e}")
            self.connection.rollback()
            
    def truncate_table(self):
        try:
            self.cursor.execute("TRUNCATE TABLE raw_data")
            self.connection.commit()
            print("All data in 'raw_data' has been deleted successfully.")
        except pymysql.MySQLError as e:
            print(f"Error while truncating table: {e}")
            self.connection.rollback()

        
def main():
    uploader = AWSUploader()
    # uploader.upload_csv("raw_data.csv") # now all datas in table
    # uploader.truncate_table() # deleting all datas from table
    
    # implent from here

if __name__ == '__main__':
    main()