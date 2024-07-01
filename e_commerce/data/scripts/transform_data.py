import pandas as pd

def clean_and_transform_data(data):
    # Handle missing values and duplicates
    for df in data.values():
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
    
    # Join tables
    merged_df = pd.merge(data['customers'], data['orders'], on='customer_id')
    merged_df = pd.merge(merged_df, data['order_items'], on='order_id')
    merged_df = pd.merge(merged_df, data['products'], on='product_id')
    merged_df = pd.merge(merged_df, data['categories'], on='category_id')
    merged_df = pd.merge(merged_df, data['reviews'], on=['product_id', 'customer_id'])
    
    # Aggregate data
    aggregated_df = merged_df.groupby('customer_id').agg({
        'name': 'first',
        'email': 'first',
        'country': 'first',
        'total_amount': 'sum',
        'order_id': 'count',
        'quantity': 'sum',
        'rating': 'mean'
    }).reset_index()
    
    aggregated_df.columns = ['customer_id', 'name', 'email', 'country', 'total_amount_spent', 
                             'total_orders', 'total_products_ordered', 'average_rating']
    
    aggregated_df['average_order_value'] = aggregated_df['total_amount_spent'] / aggregated_df['total_orders']
    
    return merged_df, aggregated_df

if __name__ == "__main__":
    # Load the extracted data (assuming it was saved to CSV files)
    data = {
        'customers': pd.read_csv('customers.csv'),
        'orders': pd.read_csv('orders.csv'),
        'order_items': pd.read_csv('order_items.csv'),
        'products': pd.read_csv('products.csv'),
        'categories': pd.read_csv('categories.csv'),
        'reviews': pd.read_csv('reviews.csv')
    }
    
    merged_df, aggregated_df = clean_and_transform_data(data)
    
    # Save both merged and aggregated data
    merged_df.to_csv('merged_data.csv', index=False)
    aggregated_df.to_csv('aggregated_data.csv', index=False)
    
    print("Merged and aggregated data saved successfully.")