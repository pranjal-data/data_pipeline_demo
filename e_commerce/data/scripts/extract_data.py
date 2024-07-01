import os
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_NAME = 'e_commerce'

# Create SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

def extract_table(table_name):
    """
    Extract data from a specific table
    """
    query = f"SELECT * FROM {table_name}"
    with engine.connect() as connection:
        result = connection.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def extract_all_data():
    """
    Extract data from all tables
    """
    tables = ['customers', 'categories', 'products', 'orders', 'order_items', 'reviews']
    data = {}
    for table in tables:
        data[table] = extract_table(table)
    return data

if __name__ == "__main__":
    extracted_data = extract_all_data()
    
    # Print the first few rows of each extracted dataframe
    for table, df in extracted_data.items():
        print(f"\n{table.upper()}:")
        print(df.head())
        print(f"Total rows: {len(df)}")

    # Optionally, save the extracted data to CSV files
    for table, df in extracted_data.items():
        df.to_csv(f"{table}.csv", index=False)
        print(f"Saved {table}.csv")