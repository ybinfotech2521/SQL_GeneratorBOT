# backend/fix_foreign_keys.py
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

def fix_foreign_keys():
    """Fix foreign key constraints after normalization"""
    
    print("🔧 Fixing foreign key constraints...")
    
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        try:
            # 1. Drop existing constraints if they exist
            conn.execute(text("""
                ALTER TABLE orders DROP CONSTRAINT IF EXISTS fk_orders_customers;
                ALTER TABLE order_items DROP CONSTRAINT IF EXISTS fk_order_items_orders;
                ALTER TABLE order_items DROP CONSTRAINT IF EXISTS fk_order_items_products;
            """))
            print("✅ Dropped existing constraints (if any)")
            
            # 2. Ensure primary keys exist
            conn.execute(text("""
                ALTER TABLE customers ADD PRIMARY KEY (customer_id);
                ALTER TABLE products ADD PRIMARY KEY (product_id);
                ALTER TABLE orders ADD PRIMARY KEY (order_id);
            """))
            print("✅ Added primary key constraints")
            
            # 3. Now add foreign keys
            conn.execute(text("""
                ALTER TABLE orders 
                ADD CONSTRAINT fk_orders_customers 
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id);
            """))
            
            conn.execute(text("""
                ALTER TABLE order_items 
                ADD CONSTRAINT fk_order_items_orders 
                FOREIGN KEY (order_id) REFERENCES orders(order_id);
            """))
            
            conn.execute(text("""
                ALTER TABLE order_items 
                ADD CONSTRAINT fk_order_items_products 
                FOREIGN KEY (product_id) REFERENCES products(product_id);
            """))
            
            print("✅ Foreign key constraints added successfully!")
            
            # 4. Verify the constraints
            result = conn.execute(text("""
                SELECT
                    tc.table_name, 
                    kcu.column_name, 
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name 
                FROM 
                    information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public';
            """))
            
            foreign_keys = result.fetchall()
            print(f"\n📋 Foreign Key Relationships:")
            for fk in foreign_keys:
                print(f"   {fk[0]}.{fk[1]} → {fk[2]}.{fk[3]}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error fixing foreign keys: {e}")
            return False

if __name__ == "__main__":
    fix_foreign_keys()