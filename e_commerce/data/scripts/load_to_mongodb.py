from pymongo import MongoClient
import pandas as pd
import json

def load_to_mongodb(aggregated_data, insights):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['ecommerce_data']
    
    # Load aggregated data
    aggregated_collection = db['aggregated_data']
    aggregated_collection.insert_many(aggregated_data.to_dict('records'))
    
    # Load insights
    insights_collection = db['insights']
    for name, data in insights.items():
        insights_collection.insert_one({
            'name': name,
            'data': json.loads(data.to_json())
        })

if __name__ == "__main__":
    aggregated_data = pd.read_csv('aggregated_data.csv')
    
    insights = {
        'top_5_customers': pd.read_csv('top_5_customers.csv'),
        'top_5_products': pd.read_csv('top_5_products.csv'),
        'avg_rating_by_category': pd.read_csv('avg_rating_by_category.csv'),
        'monthly_sales_trend': pd.read_csv('monthly_sales_trend.csv')
    }
    
    load_to_mongodb(aggregated_data, insights)
    print("Data loaded to MongoDB successfully")