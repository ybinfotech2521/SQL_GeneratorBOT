# backend/verify_normalized_db.py
import asyncio
import sys
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

def verify_database():
    """Verify normalized database structure and relationships"""
    
    print("=" * 60)
    print("🔍 VERIFYING NORMALIZED DATABASE")
    print("=" * 60)
    
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        # 1. Check table counts
        print("\n📊 TABLE RECORD COUNTS:")
        tables = ['customers', 'products', 'orders', 'order_items']
        
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.fetchone()[0]
            print(f"   {table:15} → {count:>9,} records")
        
        # 2. Check foreign key relationships
        print("\n🔗 FOREIGN KEY RELATIONSHIPS:")
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
            AND tc.table_schema = 'public'
            ORDER BY tc.table_name;
        """))
        
        foreign_keys = result.fetchall()
        if foreign_keys:
            for fk in foreign_keys:
                print(f"   {fk[0]}.{fk[1]} → {fk[2]}.{fk[3]}")
        else:
            print("   ⚠️ No foreign key constraints found!")
        
        # 3. Test sample queries
        print("\n🧪 TESTING SAMPLE QUERIES:")
        
        # Query 1: Customer count by country
        print("\n   1. Customers by country (top 5):")
        result = conn.execute(text("""
            SELECT country, COUNT(*) as customer_count
            FROM customers
            GROUP BY country
            ORDER BY customer_count DESC
            LIMIT 5;
        """))
        
        for row in result.fetchall():
            print(f"      {row[0]:20} → {row[1]:>5} customers")
        
        # Query 2: Top selling products
        print("\n   2. Top selling products (by quantity):")
        result = conn.execute(text("""
            SELECT p.name, SUM(oi.quantity) as total_quantity
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.status = 'completed'
            GROUP BY p.product_id, p.name
            ORDER BY total_quantity DESC
            LIMIT 5;
        """))
        
        for row in result.fetchall():
            product_name = str(row[0])[:40] + "..." if len(str(row[0])) > 40 else row[0]
            print(f"      {product_name:43} → {row[1]:>8,} units")
        
        # Query 3: Customer lifetime value
        print("\n   3. Top customers by spending:")
        result = conn.execute(text("""
            SELECT c.customer_id, c.country, 
                   SUM(o.total_amount) as total_spent,
                   COUNT(o.order_id) as order_count
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE o.status = 'completed'
            GROUP BY c.customer_id, c.country
            ORDER BY total_spent DESC
            LIMIT 5;
        """))
        
        for row in result.fetchall():
            print(f"      Customer {row[0]} ({row[1]}): ${row[2]:>9,.2f} in {row[3]} orders")
        
        # Query 4: Test JOIN across all 4 tables
        print("\n   4. Complex JOIN test (all 4 tables):")
        result = conn.execute(text("""
            SELECT c.customer_id, c.country, 
                   o.order_id, o.order_date,
                   p.name as product_name,
                   oi.quantity, oi.unit_price,
                   (oi.quantity * oi.unit_price) as line_total
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN products p ON oi.product_id = p.product_id
            WHERE o.status = 'completed'
            ORDER BY o.order_date DESC
            LIMIT 3;
        """))
        
        rows = result.fetchall()
        print(f"      ✅ Query successful! Returned {len(rows)} rows")
        if rows:
            print(f"      Sample: Customer {rows[0][0]} ordered {rows[0][4][:30]}...")
        
        # 4. Check data consistency
        print("\n🔍 DATA CONSISTENCY CHECKS:")
        
        # Check for orphaned records
        result = conn.execute(text("""
            SELECT COUNT(*) as orphaned_orders
            FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL;
        """))
        orphaned_orders = result.fetchone()[0]
        print(f"   Orders without customer: {orphaned_orders}")
        
        result = conn.execute(text("""
            SELECT COUNT(*) as orphaned_items
            FROM order_items oi
            LEFT JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_id IS NULL;
        """))
        orphaned_items = result.fetchone()[0]
        print(f"   Order items without order: {orphaned_items}")
        
        result = conn.execute(text("""
            SELECT COUNT(*) as orphaned_products
            FROM order_items oi
            LEFT JOIN products p ON oi.product_id = p.product_id
            WHERE p.product_id IS NULL;
        """))
        orphaned_products = result.fetchone()[0]
        print(f"   Order items without product: {orphaned_products}")
    
    print("\n" + "=" * 60)
    print("✅ VERIFICATION COMPLETE")
    print("=" * 60)
    
    if foreign_keys and orphaned_orders == 0 and orphaned_items == 0:
        print("\n🎉 Your normalized database is READY for AI queries!")
        print("   Next: Test with your AI chatbot using multi-table questions.")
    else:
        print("\n⚠️  Some issues detected. Run fix_foreign_keys.py first.")
    
    return foreign_keys

async def test_ai_integration():
    """Test AI integration with normalized database"""
    print("\n" + "=" * 60)
    print("🤖 TESTING AI INTEGRATION")
    print("=" * 60)
    
    try:
        from app.utils.schema_builder import get_detailed_schema, format_schema_for_prompt
        from app.llm.sql_generator import generate_sql
        from app.llm.answer_formatter import format_answer
        from app.db.database import SessionLocal
        from sqlalchemy import text
        
        # Get schema
        schema = get_detailed_schema()
        print(f"\n📋 Schema loaded: {len(schema['tables'])} tables")
        
        # Test questions for normalized database
        test_questions = [
            "Show me customers from Germany and their total orders",
            "What products did customer 17850 buy?",
            "Calculate monthly revenue by country",
            "Show top 5 products by sales quantity"
        ]
        
        for i, question in enumerate(test_questions, 1):
            print(f"\n[{i}] Question: '{question}'")
            
            try:
                # Generate SQL
                sql = await generate_sql(question, schema)
                print(f"   SQL: {sql[:100]}...")
                
                # Check if it has JOINs
                if "JOIN" in sql.upper():
                    print("   ✅ Contains JOINs (good for normalized DB)")
                
                # Execute query
                with SessionLocal() as session:
                    result = session.execute(text(sql))
                    rows = [dict(r._mapping) for r in result.fetchall()]
                    
                    if rows:
                        print(f"   📊 Results: {len(rows)} rows")
                        
                        # Format answer
                        answer = await format_answer(question, sql, rows[:3], schema)
                        print(f"   💬 Answer: {answer[:120]}...")
                    else:
                        print(f"   ⚠️  No results")
                        
            except Exception as e:
                print(f"   ❌ Error: {str(e)[:100]}")
    
    except ImportError as e:
        print(f"❌ Cannot import modules: {e}")
        print("   Make sure you've updated sql_generator.py and answer_formatter.py")
    except Exception as e:
        print(f"❌ Test failed: {e}")

async def main():
    """Run all verification tests"""
    # First verify database structure
    foreign_keys = verify_database()
    
    # Only test AI if database verification passed
    if foreign_keys:
        print("\n" + "=" * 60)
        response = input("Test AI integration with normalized database? (y/n): ")
        
        if response.lower() == 'y':
            await test_ai_integration()

if __name__ == "__main__":
    asyncio.run(main())