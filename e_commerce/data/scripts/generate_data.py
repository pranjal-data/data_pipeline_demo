import mysql.connector
from mysql.connector import Error
from datetime import date, timedelta
import random

# Database connection parameters
config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password'
}

# SQL statements to create tables
create_database = "CREATE DATABASE IF NOT EXISTS e_commerce"

create_customers = """
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    country VARCHAR(50) NOT NULL
)
"""

create_categories = """
CREATE TABLE categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
)
"""

create_products = """
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category_id INT NOT NULL,
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
)
"""

create_orders = """
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
)
"""

create_order_items = """
CREATE TABLE order_items (
    item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
)
"""

create_reviews = """
CREATE TABLE reviews (
    review_id INT PRIMARY KEY,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    review_date DATE NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
)
"""

def create_database_and_tables():
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # Create database
        cursor.execute(create_database)
        print("Database created successfully")
        
        # Switch to the new database
        cursor.execute("USE e_commerce")
        
        # Create tables
        cursor.execute(create_customers)
        cursor.execute(create_categories)
        cursor.execute(create_products)
        cursor.execute(create_orders)
        cursor.execute(create_order_items)
        cursor.execute(create_reviews)
        
        print("Tables created successfully")
        
    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def insert_sample_data():
    try:
        conn = mysql.connector.connect(**config, database='e_commerce')
        cursor = conn.cursor()
        
        # Insert sample data into customers
        for i in range(1, 11):
            cursor.execute("""
            INSERT INTO customers (customer_id, name, email, country)
            VALUES (%s, %s, %s, %s)
            """, (i, f"Customer {i}", f"customer{i}@email.com", f"Country {i}"))
        
        # Insert sample data into categories
        for i in range(1, 11):
            cursor.execute("""
            INSERT INTO categories (category_id, category_name)
            VALUES (%s, %s)
            """, (i, f"Category {i}"))
        
        # Insert sample data into products
        for i in range(1, 11):
            cursor.execute("""
            INSERT INTO products (product_id, product_name, category_id)
            VALUES (%s, %s, %s)
            """, (i, f"Product {i}", random.randint(1, 10)))
        
        # Insert sample data into orders
        for i in range(1, 11):
            cursor.execute("""
            INSERT INTO orders (order_id, customer_id, order_date, total_amount, status)
            VALUES (%s, %s, %s, %s, %s)
            """, (i, random.randint(1, 10), date.today() - timedelta(days=random.randint(1, 30)), 
                  round(random.uniform(10.0, 1000.0), 2), random.choice(['Pending', 'Shipped', 'Delivered'])))
        
        # Insert sample data into order_items
        for i in range(1, 11):
            cursor.execute("""
            INSERT INTO order_items (item_id, order_id, product_id, quantity, price)
            VALUES (%s, %s, %s, %s, %s)
            """, (i, random.randint(1, 10), random.randint(1, 10), random.randint(1, 5), 
                  round(random.uniform(10.0, 100.0), 2)))
        
        # Insert sample data into reviews
        for i in range(1, 11):
            cursor.execute("""
            INSERT INTO reviews (review_id, product_id, customer_id, rating, review_date)
            VALUES (%s, %s, %s, %s, %s)
            """, (i, random.randint(1, 10), random.randint(1, 10), random.randint(1, 5), 
                  date.today() - timedelta(days=random.randint(1, 30))))
        
        conn.commit()
        print("Sample data inserted successfully")
        
    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_database_and_tables()
    insert_sample_data()