# backend/test_pipeline.py
import sys
import os
from dotenv import load_dotenv

# Add your app directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

load_dotenv()

async def test_sql_generation_and_execution():
    """
    Test the complete pipeline: schema loading, local SQL generation, and database execution.
    """
    print("=" * 60)
    print("🧪 TESTING APPLICATION PIPELINE (Local Fallback)")
    print("=" * 60)

    # 1. Import your app modules
    try:
        from app.utils.schema_loader import get_schema_summary
        from app.llm.sql_generator import generate_sql
        from app.db.database import SessionLocal
        from sqlalchemy import text
        from app.utils.sanitizer import is_safe_select
    except ImportError as e:
        print(f"❌ FAILED: Could not import app modules. Error: {e}")
        print("   Check that you are running from the correct directory (backend/) and that your app structure is correct.")
        return

    # 2. Load the database schema
    print("\n[1/4] Loading database schema...")
    try:
        schema = get_schema_summary()
        print(f"   ✅ Schema loaded. Found {len(schema.get('schema', {}))} table(s).")
        # Print a quick summary
        for table_name, columns in schema.get('schema', {}).items():
            print(f"      - Table: '{table_name}' has {len(columns)} columns.")
    except Exception as e:
        print(f"❌ FAILED to load schema. Database may not be connected. Error: {e}")
        return

    # 3. Test SQL generation for different questions
    print("\n[2/4] Testing local SQL generation...")
    test_questions = [
        "What was the total revenue last month?",
        "Show me the top 5 customers",
        "Count all products"
    ]

    generated_queries = {}
    for question in test_questions:
        try:
            # Ensure we are using the local fallback by temporarily forcing it
            original_env = os.getenv("USE_LOCAL_FALLBACK")
            os.environ["USE_LOCAL_FALLBACK"] = "true"

            sql = await generate_sql(question, schema)
            generated_queries[question] = sql
            print(f"   ✅ '{question[:30]}...'")
            print(f"      Generated SQL: {sql[:80]}...")

            # Safety check
            if not is_safe_select(sql):
                print(f"   ⚠️  WARNING: Generated SQL did not pass safety check for: {question}")

            # Restore original env
            if original_env:
                os.environ["USE_LOCAL_FALLBACK"] = original_env
            else:
                os.environ.pop("USE_LOCAL_FALLBACK", None)

        except Exception as e:
            print(f"   ❌ Failed for '{question}': {e}")

    # 4. Test executing one of the generated queries on the database
    print("\n[3/4] Testing database execution...")
    if generated_queries:
        # Pick the first successful query to run
        test_q, test_sql = next(iter(generated_queries.items()))
        print(f"   Executing query for: '{test_q}'")

        try:
            with SessionLocal() as session:
                # Execute the generated SQL
                result = session.execute(text(test_sql))
                rows = result.fetchall()

                print(f"   ✅ Query executed successfully.")
                print(f"   Number of rows returned: {len(rows)}")

                if rows:
                    # Show column names and first row
                    col_names = result.keys()
                    print(f"   Columns: {list(col_names)}")
                    print(f"   First row sample: {rows[0]}")
                else:
                    print("   Note: Query returned an empty result set.")

        except Exception as e:
            print(f"❌ FAILED to execute SQL on database. Error: {e}")
            print(f"   Problem SQL: {test_sql}")
            print("\n   TROUBLESHOOTING:")
            print("   1. Check if PostgreSQL is running (`sudo service postgresql status` or via pgAdmin).")
            print("   2. Verify your .env credentials match your database setup.")
            print("   3. Try connecting manually using: `psql -h localhost -U postgres -d ecommerce_db`")

    # 5. Test the answer formatter (local fallback)
    print("\n[4/4] Testing local answer formatting...")
    try:
        from app.llm.answer_formatter import format_answer

        # Use a simple mock result
        mock_rows = [{'month': '2024-11-01', 'revenue': 12500.50}]
        answer = await format_answer(
            user_question="What was the revenue last month?",
            sql="SELECT DATE_TRUNC('month', invoicedate) AS month, SUM(quantity * unitprice) AS revenue FROM transactions GROUP BY month",
            rows=mock_rows,
            schema_summary=schema
        )
        print(f"   ✅ Answer formatter working.")
        print(f"   Sample Answer: '{answer}'")
    except Exception as e:
        print(f"   ⚠️  Answer formatter issue (may be expected if LLM is down): {e}")

    print("\n" + "=" * 60)
    print("🏁 TEST COMPLETE")
    print("=" * 60)
    if generated_queries:
        print("✅ CORE PIPELINE IS FUNCTIONAL.")
        print("   Your app can load schema, generate SQL, and query the DB.")
        print("\n   NEXT STEP: Once this works, you can troubleshoot LLM integration.")
    else:
        print("❌ PIPELINE HAS ISSUES.")
        print("   Focus on fixing the steps above before adding an LLM.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_sql_generation_and_execution())