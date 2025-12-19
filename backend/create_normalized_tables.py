# backend/create_normalized_tables.py
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def create_normalized_database():
    """
    Transform the single transactions table into 4 normalized tables.
    """
    print("📦 Starting database normalization...")
    
    # 1. Load your original data
    try:
        # Adjust the path to your CSV file
        df = pd.read_csv('E:/ecom-llm-analytics/data/ecommerce-data.csv', encoding='ISO-8859-1')
        print(f"✅ Loaded original data: {len(df)} rows, {len(df.columns)} columns")
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return
    
    # 2. Clean the data
    print("\n🧹 Cleaning data...")
    df_clean = df.copy()
    
    # Remove rows with missing CustomerID (they represent non-customer transactions)
    df_clean = df_clean[df_clean['CustomerID'].notna()]
    df_clean['CustomerID'] = df_clean['CustomerID'].astype(str).str.strip()
    
    # Handle negative quantities (returns/cancellations)
    df_clean['Quantity'] = pd.to_numeric(df_clean['Quantity'], errors='coerce')
    df_clean['UnitPrice'] = pd.to_numeric(df_clean['UnitPrice'], errors='coerce')
    
    # Filter out cancelled orders (where InvoiceNo starts with 'C')
    df_clean = df_clean[~df_clean['InvoiceNo'].astype(str).str.startswith('C')]
    
    print(f"   Cleaned data: {len(df_clean)} rows remaining")
    
    # 3. Create database connection
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)
    
    # 4. CREATE TABLE: Customers
    print("\n👥 Creating customers table...")
    customers = df_clean[['CustomerID', 'Country']].copy()
    customers = customers.drop_duplicates(subset=['CustomerID'])
    customers.columns = ['customer_id', 'country']
    
    # Add dummy data for missing columns (for demo purposes)
    customers['name'] = 'Customer_' + customers['customer_id']
    customers['email'] = customers['customer_id'] + '@example.com'
    customers['registration_date'] = pd.to_datetime('2010-01-01')  # Default date
    
    customers.to_sql('customers', engine, if_exists='replace', index=False)
    print(f"   ✅ Created {len(customers)} customer records")
    
    # 5. CREATE TABLE: Products
    print("\n📦 Creating products table...")
    products = df_clean[['StockCode', 'Description', 'UnitPrice']].copy()
    products = products.drop_duplicates(subset=['StockCode'])
    products.columns = ['product_id', 'name', 'unit_price']
    
    # Add dummy data for missing columns
    products['category'] = 'General'
    products['supplier'] = 'Default Supplier'
    
    products.to_sql('products', engine, if_exists='replace', index=False)
    print(f"   ✅ Created {len(products)} product records")
    
    # 6. CREATE TABLE: Orders
    print("\n📋 Creating orders table...")
    
    # Group by InvoiceNo to create orders
    orders = df_clean.groupby('InvoiceNo').agg({
        'CustomerID': 'first',
        'InvoiceDate': 'first',
        'Quantity': 'sum',
        'UnitPrice': lambda x: sum(x * df_clean.loc[x.index, 'Quantity'])
    }).reset_index()
    
    orders.columns = ['order_id', 'customer_id', 'order_date', 'total_quantity', 'total_amount']
    
    # Clean order_date
    orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')
    
    # Add status (simplified: if total_quantity > 0 then 'completed')
    orders['status'] = 'completed'
    
    orders.to_sql('orders', engine, if_exists='replace', index=False)
    print(f"   ✅ Created {len(orders)} order records")
    
    # 7. CREATE TABLE: Order Items
    print("\n🛍️ Creating order_items table...")
    
    # Get order_id to InvoiceNo mapping
    order_mapping = orders[['order_id']].reset_index()
    
    # Create order_items from original line items
    order_items = df_clean[['InvoiceNo', 'StockCode', 'Quantity', 'UnitPrice']].copy()
    order_items.columns = ['order_id', 'product_id', 'quantity', 'unit_price']
    
    # Merge to get the correct order_id
    order_items = order_items.merge(orders[['order_id']], left_on='order_id', right_on='order_id', how='inner')
    
    # Reset index to create order_item_id
    order_items = order_items.reset_index(drop=True)
    order_items.index.name = 'order_item_id'
    order_items = order_items.reset_index()
    order_items['order_item_id'] = order_items['order_item_id'] + 1  # Start from 1
    
    order_items.to_sql('order_items', engine, if_exists='replace', index=False)
    print(f"   ✅ Created {len(order_items)} order item records")
    
    # 8. Add Foreign Key constraints
    print("\n🔗 Adding foreign key constraints...")
    with engine.connect() as conn:
        try:
            # Add FK: orders.customer_id → customers.customer_id
            conn.execute(text("""
                ALTER TABLE orders 
                ADD CONSTRAINT fk_orders_customers 
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            """))
            
            # Add FK: order_items.order_id → orders.order_id
            conn.execute(text("""
                ALTER TABLE order_items 
                ADD CONSTRAINT fk_order_items_orders 
                FOREIGN KEY (order_id) REFERENCES orders(order_id)
            """))
            
            # Add FK: order_items.product_id → products.product_id
            conn.execute(text("""
                ALTER TABLE order_items 
                ADD CONSTRAINT fk_order_items_products 
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            """))
            
            print("   ✅ Foreign key constraints added")
        except Exception as e:
            print(f"   ⚠️ Could not add foreign keys (might already exist): {e}")
    
    print("\n" + "="*60)
    print("🎉 DATABASE NORMALIZATION COMPLETE!")
    print("="*60)
    print(f"📊 Summary:")
    print(f"   • Customers: {len(customers)} records")
    print(f"   • Products: {len(products)} records")
    print(f"   • Orders: {len(orders)} records")
    print(f"   • Order Items: {len(order_items)} records")
    print("\n✅ Your database now has 4 related tables ready for AI queries!")

if __name__ == "__main__":
    create_normalized_database()