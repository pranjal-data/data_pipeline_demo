import pandas as pd

def generate_insights(merged_df, aggregated_df):
    insights = {}
    
    # Top 5 customers by total amount spent
    insights['top_5_customers'] = aggregated_df.nlargest(5, 'total_amount_spent')[['name', 'total_amount_spent']]
    
    # Top 5 products by number of orders
    product_orders = merged_df.groupby('product_name')['order_id'].nunique().sort_values(ascending=False)
    insights['top_5_products'] = product_orders.head(5)
    
    # Average rating of products by category
    insights['avg_rating_by_category'] = merged_df.groupby('category_name')['rating'].mean()
    
    # Monthly sales trend
    merged_df['order_date'] = pd.to_datetime(merged_df['order_date'])
    monthly_sales = merged_df.groupby(merged_df['order_date'].dt.to_period('M'))['total_amount'].sum()
    insights['monthly_sales_trend'] = monthly_sales
    
    return insights

if __name__ == "__main__":
    # Load the merged and aggregated data
    merged_df = pd.read_csv('merged_data.csv')
    aggregated_df = pd.read_csv('aggregated_data.csv')
    
    insights = generate_insights(merged_df, aggregated_df)
    
    for name, insight in insights.items():
        print(f"\n{name.upper()}:")
        print(insight)
        insight.to_csv(f'{name}.csv')
    
    print("Insights generated and saved successfully.")