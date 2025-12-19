# backend/test_ai_normalized.py
import asyncio
import sys
import os
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

async def test_ai_chatbot():
    """Test AI chatbot with normalized database"""
    
    print("🤖 TESTING AI CHATBOT WITH NORMALIZED DATABASE")
    print("=" * 60)
    
    try:
        # Import all modules
        from app.utils.schema_builder import get_detailed_schema, format_schema_for_prompt
        from app.llm.sql_generator import generate_sql
        from app.llm.answer_formatter import format_answer
        from app.db.database import SessionLocal
        from sqlalchemy import text
        from app.utils.sanitizer import is_safe_select
        
        print("✅ All modules imported successfully")
        
        # Get schema
        schema = get_detailed_schema()
        print(f"📋 Schema loaded: {len(schema['tables'])} tables")
        
        # Test questions that require JOINs
        test_questions = [
            "Show me customers from Germany and their order counts",
            "What products did customer 17850 buy?",
            "Calculate total revenue by month",
            "Show top 5 selling products",
            "Which countries have the most customers?"
        ]
        
        print(f"\n🧪 Testing {len(test_questions)} multi-table questions...")
        
        for i, question in enumerate(test_questions, 1):
            print(f"\n[{i}] Question: '{question}'")
            
            try:
                # Generate SQL using AI
                print("   🤖 Generating SQL...")
                sql = await generate_sql(question, schema)
                
                if not sql or "SELECT" not in sql.upper():
                    print(f"   ❌ Invalid SQL generated")
                    continue
                
                # Check safety
                if not is_safe_select(sql):
                    print(f"   ⚠️  SQL safety check failed")
                    continue
                
                # Check for JOINs (should have them for multi-table questions)
                has_joins = "JOIN" in sql.upper()
                print(f"   📝 SQL: {sql[:100]}...")
                if has_joins:
                    print("   ✅ Contains JOINs (good for normalized DB)")
                else:
                    print("   ⚠️  No JOINs in SQL")
                
                # Execute the query
                print("   🗄️  Executing query...")
                with SessionLocal() as session:
                    result = session.execute(text(sql))
                    rows = [dict(r._mapping) for r in result.fetchall()]
                    
                    if rows:
                        print(f"   📊 Query returned {len(rows)} rows")
                        
                        # Format answer using AI
                        print("   💬 Formatting answer with AI...")
                        answer = await format_answer(question, sql, rows[:5], schema)
                        
                        if answer:
                            print(f"   🎯 AI Answer: {answer}")
                        else:
                            print(f"   ⚠️  AI returned empty answer")
                    else:
                        print(f"   ⚠️  Query returned no results")
                
            except Exception as e:
                print(f"   ❌ Error: {str(e)[:100]}")
        
        print("\n" + "=" * 60)
        print("✅ AI CHATBOT TEST COMPLETE!")
        print("=" * 60)
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("\n🔧 Check these files exist:")
        print("   backend/app/llm/sql_generator.py")
        print("   backend/app/llm/answer_formatter.py")
        print("   backend/app/utils/schema_builder.py")
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    asyncio.run(test_ai_chatbot())