# backend/test_normalized_queries.py
import asyncio
import sys
import os
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

async def test_normalized_queries():
    """Test multi-table queries with JOINs"""
    
    print("=" * 60)
    print("🧪 TESTING NORMALIZED DATABASE QUERIES")
    print("=" * 60)
    
    from app.utils.schema_builder import get_detailed_schema, format_schema_for_prompt
    from app.llm.sql_generator import generate_sql
    from app.llm.answer_formatter import format_answer
    from app.db.database import SessionLocal
    from sqlalchemy import text
    
    # Get enhanced schema
    schema = get_detailed_schema()
    print(f"\n📊 Database has {len(schema['tables'])} tables:")
    for table in schema['tables']:
        print(f"   • {table}")
    
    # Test questions that require JOINs
    test_questions = [
        "Show me customers and their total order amounts",
        "What products did customer 17850 buy?",
        "List all orders with customer names and countries",
        "Calculate monthly revenue by customer country",
        "Show top 5 products by sales quantity along with their categories"
    ]
    
    for i, question in enumerate(test_questions, 1):
        print(f"\n[{i}/5] Testing: '{question}'")
        
        try:
            # Generate SQL
            sql = await generate_sql(question, schema)
            print(f"   Generated SQL: {sql[:100]}...")
            
            # Check if it has JOINs (for multi-table questions)
            if "JOIN" in sql.upper():
                print(f"   ✅ Contains JOINs (good for multi-table query)")
            
            # Execute query
            with SessionLocal() as session:
                result = session.execute(text(sql))
                rows = [dict(r._mapping) for r in result.fetchall()]
                
                if rows:
                    print(f"   ✅ Returned {len(rows)} rows")
                    
                    # Format answer
                    answer = await format_answer(question, sql, rows[:3], schema)
                    print(f"   📝 Answer: {answer[:150]}...")
                else:
                    print(f"   ⚠️  No results returned")
                    
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print("\n" + "=" * 60)
    print("✅ MULTI-TABLE TESTING COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_normalized_queries())