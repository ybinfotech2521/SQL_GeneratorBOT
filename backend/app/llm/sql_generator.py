# backend/app/llm/sql_generator.py
"""
Generate SQL queries for multi-table e-commerce database using Groq API.
"""

from typing import Dict, Any
from textwrap import dedent
import os
import re
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.utils.schema_builder import get_detailed_schema, format_schema_for_prompt
from app.llm.groq_client import call_groq_chat

MAX_ROWS_DEFAULT = int(os.getenv("MAX_QUERY_ROWS", "1000"))
USE_LOCAL_FALLBACK = os.getenv("USE_LOCAL_FALLBACK", "false").lower() == "true"
FORCE_USE_LLM = os.getenv("FORCE_USE_LLM", "false").lower() == "true"

async def generate_sql(user_question: str, schema_summary: Dict[str, Any], max_tokens: int = 1024) -> str:
    """
    Generate SQL query for multi-table e-commerce database using Groq API.
    
    Args:
        user_question: Natural language question from user
        schema_summary: Database schema information
        max_tokens: Maximum tokens for LLM response
        
    Returns:
        SQL query string
    """
    # Quick bypass for testing
    if USE_LOCAL_FALLBACK:
        print("[SQL Generator] Using local fallback (bypassed LLM)")
        return generate_local_sql(user_question, schema_summary)
    
    # Get detailed schema with relationships
    try:
        detailed_schema = get_detailed_schema()
        schema_prompt = format_schema_for_prompt(detailed_schema)
    except Exception as e:
        print(f"[SQL Generator] Error loading schema: {e}")
        return generate_local_sql(user_question, schema_summary)
    
    # Enhanced system prompt for multi-table queries
    system_prompt = dedent(f"""
    You are an expert PostgreSQL SQL generator for a NORMALIZED e-commerce database.
    
    ==============================================
    COMPLETE DATABASE SCHEMA WITH ALL COLUMNS
    ==============================================
    
    1. CUSTOMERS TABLE - Stores customer information
       -------------------------------------------------
       • customer_id (VARCHAR, PRIMARY KEY) - Unique customer identifier
       • name (VARCHAR) - Customer name
       • email (VARCHAR) - Customer email address
       • country (VARCHAR) - Customer country
       • registration_date (DATE) - When customer registered
    
    2. PRODUCTS TABLE - Stores product catalog
       -------------------------------------------------
       • product_id (VARCHAR, PRIMARY KEY) - Unique product code (was StockCode)
       • name (VARCHAR) - Product description/name (was Description)
       • category (VARCHAR) - Product category
       • unit_price (DECIMAL) - Product price (was UnitPrice)
       • supplier (VARCHAR) - Product supplier
    
    3. ORDERS TABLE - Stores order headers
       -------------------------------------------------
       • order_id (VARCHAR, PRIMARY KEY) - Unique order number (was InvoiceNo)
       • customer_id (VARCHAR, FOREIGN KEY → customers.customer_id) - Customer who placed order
       • order_date (TIMESTAMP) - When order was placed (was InvoiceDate)
       • total_amount (DECIMAL) - Total order value
       • status (VARCHAR) - Order status: 'pending', 'completed', 'cancelled', 'shipped'
    
    4. ORDER_ITEMS TABLE - Stores individual line items in orders
       -----------------------------------------------------------
       • order_item_id (SERIAL, PRIMARY KEY) - Auto-incrementing line item ID
       • order_id (VARCHAR, FOREIGN KEY → orders.order_id) - Which order this belongs to
       • product_id (VARCHAR, FOREIGN KEY → products.product_id) - Which product was ordered
       • quantity (INTEGER) - Quantity ordered (was Quantity)
       • unit_price (DECIMAL) - Price at time of order
    
    ==============================================
    RELATIONSHIPS (FOREIGN KEY MAPPINGS)
    ==============================================
    
    ONE-TO-MANY RELATIONSHIPS:
    • One CUSTOMER → Many ORDERS (via customers.customer_id = orders.customer_id)
    • One ORDER → Many ORDER_ITEMS (via orders.order_id = order_items.order_id)
    • One PRODUCT → Many ORDER_ITEMS (via products.product_id = order_items.product_id)
    
    JOIN PATHS:
    • Customer → Orders: JOIN customers c ON c.customer_id = orders.customer_id
    • Order → Items: JOIN orders o ON o.order_id = order_items.order_id
    • Item → Product: JOIN order_items oi ON oi.product_id = products.product_id
    
    ==============================================
    COLUMN MAPPINGS FROM ORIGINAL DATASET
    ==============================================
    Original CSV columns → New database columns:
    • InvoiceNo → orders.order_id
    • StockCode → products.product_id
    • Description → products.name
    • Quantity → order_items.quantity
    • InvoiceDate → orders.order_date
    • UnitPrice → products.unit_price (catalog) AND order_items.unit_price (transaction)
    • CustomerID → customers.customer_id
    • Country → customers.country
    
    ==============================================
    KEY BUSINESS RULES
    ==============================================
    
    REVENUE CALCULATIONS:
    1. Revenue = SUM(order_items.quantity * order_items.unit_price)
    2. Only include orders with status = 'completed' in revenue reports
    3. Filter out cancelled orders: WHERE orders.status != 'cancelled'
    
    DATA QUALITY RULES:
    1. Some CustomerID values may be NULL in original data - these are not in customers table
    2. Negative quantity values in original data represent returns/refunds
    3. Use ABS(quantity) for quantity analysis: ABS(order_items.quantity)
    4. For revenue, only use positive quantities: WHERE order_items.quantity > 0
    
    AGGREGATION RULES:
    1. When grouping by date, use: DATE_TRUNC('month', orders.order_date)
    2. For customer analysis, group by: customers.customer_id
    3. For product analysis, group by: products.product_id
    
    ==============================================
    QUERY TEMPLATES & EXAMPLES
    ==============================================
    
    TEMPLATE 1: Customer order history
    ----------------------------------
    SELECT 
        c.customer_id,
        c.name,
        o.order_id,
        o.order_date,
        o.total_amount
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE c.customer_id = 'SPECIFIC_CUSTOMER_ID'
    ORDER BY o.order_date DESC
    LIMIT {MAX_ROWS_DEFAULT};
    
    TEMPLATE 2: Product sales report
    --------------------------------
    SELECT 
        p.product_id,
        p.name,
        p.category,
        SUM(oi.quantity) as total_quantity_sold,
        SUM(oi.quantity * oi.unit_price) as total_revenue
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.status = 'completed'
    GROUP BY p.product_id, p.name, p.category
    ORDER BY total_revenue DESC
    LIMIT {MAX_ROWS_DEFAULT};
    
    TEMPLATE 3: Monthly revenue by country
    --------------------------------------
    SELECT 
        c.country,
        DATE_TRUNC('month', o.order_date) as month,
        COUNT(DISTINCT o.order_id) as order_count,
        SUM(oi.quantity * oi.unit_price) as monthly_revenue
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status = 'completed'
    GROUP BY c.country, DATE_TRUNC('month', o.order_date)
    ORDER BY month DESC, monthly_revenue DESC
    LIMIT {MAX_ROWS_DEFAULT};
    
    TEMPLATE 4: Customer lifetime value
    -----------------------------------
    SELECT 
        c.customer_id,
        c.name,
        c.country,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(o.total_amount) as lifetime_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.status = 'completed'
    GROUP BY c.customer_id, c.name, c.country
    ORDER BY lifetime_value DESC
    LIMIT {MAX_ROWS_DEFAULT};
    
    ==============================================
    CRITICAL QUERY RULES
    ==============================================
    
    1. COLUMN REFERENCE RULE: Always prefix columns with table alias
       ✅ CORRECT: SELECT c.customer_id, o.order_date
       ❌ WRONG: SELECT customer_id, order_date
    
    2. JOIN RULE: Use table aliases consistently
       ✅ CORRECT: FROM customers c JOIN orders o ON c.customer_id = o.customer_id
       ❌ WRONG: FROM customers JOIN orders ON customers.customer_id = orders.customer_id
    
    3. GROUP BY RULE: Include all non-aggregated columns in GROUP BY
       ✅ CORRECT: SELECT c.country, SUM(o.total_amount) GROUP BY c.country
       ❌ WRONG: SELECT c.country, c.name, SUM(o.total_amount) GROUP BY c.country
    
    4. LIMIT RULE: Always include LIMIT unless user asks for "all records"
    
    5. DATE RULE: Use proper date functions for time-based queries
    
    6. STATUS FILTER: Filter completed orders for financial calculations
    
    ==============================================
    OUTPUT REQUIREMENTS
    ==============================================
    
    RETURN ONLY THE SQL QUERY WITH THESE CHARACTERISTICS:
    1. Valid PostgreSQL syntax
    2. Proper table aliases (c, o, oi, p)
    3. Column names fully qualified with table alias
    4. Appropriate JOIN conditions
    5. LIMIT clause included
    6. No comments, no explanations, no markdown
    7. No trailing semicolon (optional)
    
    YOUR RESPONSE MUST BE ONLY THE SQL QUERY.
    """).strip()
    
    # Add schema details
    full_system_prompt = f"{system_prompt}\n\nDATABASE SCHEMA DETAILS:\n{schema_prompt}"
    
    # User prompt
    user_prompt = dedent(f"""
    USER QUESTION:
    {user_question}
    
    Generate a PostgreSQL SELECT query that answers this question accurately.
    Use proper JOINs based on the table relationships.
    Return ONLY the SQL query, nothing else.
    """).strip()
    
    messages = [
        {"role": "system", "content": full_system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    try:
        print(f"[SQL Generator] Calling Groq API for: {user_question[:50]}...")
        sql = await call_groq_chat(
            messages=messages,
            max_tokens=max_tokens,
            temperature=0.0,
            stop=["```", "Explanation:", "Here's", "The query"]
        )
        
        if not sql or "SELECT" not in sql.upper():
            print("[SQL Generator] Invalid SQL returned, using fallback")
            return generate_local_sql(user_question, detailed_schema)
        
        # Clean up the SQL
        sql = clean_sql(sql)
        
        # Validate it has proper structure for multi-table questions
        if requires_joins(user_question) and "JOIN" not in sql.upper():
            print("[SQL Generator] Warning: Complex question but no JOINs in SQL")
            # Try to fix simple cases
            sql = attempt_join_fix(sql, user_question, detailed_schema)
        
        print(f"[SQL Generator] Generated SQL: {sql[:150]}...")
        return sql
        
    except Exception as e:
        print(f"[SQL Generator] Groq API error: {e}. Using local fallback.")
        return generate_local_sql(user_question, detailed_schema)

def clean_sql(sql: str) -> str:
    """Clean SQL output from LLM"""
    if not sql:
        return ""
    
    # Remove markdown code blocks
    sql = re.sub(r'```sql\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'```\s*', '', sql)
    
    # Remove leading/trailing whitespace and semicolons
    sql = sql.strip().rstrip(';')
    
    # Ensure it starts with SELECT
    if not re.match(r'^\s*SELECT\b', sql, re.IGNORECASE):
        # Try to extract SQL from text
        match = re.search(r'(SELECT\s+.+?(?=LIMIT|\Z))', sql, re.IGNORECASE | re.DOTALL)
        if match:
            sql = match.group(1).strip()
        else:
            sql = ""
    
    # Add LIMIT if missing and not an aggregate-only query
    if "LIMIT" not in sql.upper() and not re.search(r'GROUP\s+BY|COUNT\(|SUM\(|AVG\(', sql, re.IGNORECASE):
        sql = f"{sql} LIMIT {MAX_ROWS_DEFAULT}"
    
    return sql

def requires_joins(question: str) -> bool:
    """Detect if question requires multi-table JOINs"""
    question_lower = question.lower()
    
    # Keywords indicating multi-table queries
    multi_table_keywords = [
        # Customer + something
        ("customer", ["order", "product", "buy", "purchase", "history", "spent"]),
        # Product + something  
        ("product", ["customer", "who bought", "ordered by", "purchased by", "bought by"]),
        # Order + details
        ("order", ["customer", "product", "item", "detail", "with"]),
        # Combinations
        ("together", []),
        ("along with", []),
        ("including", []),
        ("show", ["customer", "product", "order"]),
        ("list", ["customer", "product", "order"])
    ]
    
    for primary, secondaries in multi_table_keywords:
        if primary in question_lower:
            if not secondaries:  # Standalone keywords like "together"
                return True
            for secondary in secondaries:
                if secondary in question_lower:
                    return True
    
    return False

def attempt_join_fix(sql: str, question: str, schema: Dict[str, Any]) -> str:
    """Attempt to fix SQL missing JOINs for complex questions"""
    question_lower = question.lower()
    
    # Simple pattern-based fixes
    if "customer" in question_lower and "order" in question_lower:
        # If SQL has customers but not orders joined
        if "FROM customers" in sql and "orders" not in sql:
            # Simple fix: add JOIN to orders
            sql = sql.replace("FROM customers", "FROM customers c\nJOIN orders o ON c.customer_id = o.customer_id")
    
    elif "product" in question_lower and ("customer" in question_lower or "who" in question_lower):
        # If SQL has products but not customers
        if "FROM products" in sql and "customers" not in sql:
            sql = sql.replace("FROM products", 
                            "FROM products p\nJOIN order_items oi ON p.product_id = oi.product_id\n"
                            "JOIN orders o ON oi.order_id = o.order_id\n"
                            "JOIN customers c ON o.customer_id = c.customer_id")
    
    return sql

def generate_local_sql(user_question: str, schema_summary: Dict[str, Any]) -> str:
    """
    Generate SQL based on keywords when LLM fails.
    Enhanced for multi-table queries.
    """
    question = user_question.lower()
    tables = schema_summary.get("tables", {})
    
    # Customer-related queries
    if any(word in question for word in ["customer", "client", "buyer"]):
        if "customers" in tables and "orders" in tables:
            if "spent" in question or "spending" in question or "total" in question:
                return f"""
                SELECT c.customer_id, c.name, c.country, 
                       SUM(o.total_amount) as total_spent,
                       COUNT(o.order_id) as order_count
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                WHERE o.status = 'completed'
                GROUP BY c.customer_id, c.name, c.country
                ORDER BY total_spent DESC
                LIMIT {MAX_ROWS_DEFAULT}
                """
            else:
                return f"""
                SELECT c.customer_id, c.name, c.email, c.country, c.registration_date
                FROM customers c
                ORDER BY c.registration_date DESC
                LIMIT {MAX_ROWS_DEFAULT}
                """
    
    # Product-related queries  
    elif any(word in question for word in ["product", "item", "stock"]):
        if "products" in tables:
            if "sold" in question or "sales" in question or "popular" in question:
                return f"""
                SELECT p.product_id, p.name, p.category, 
                       SUM(oi.quantity) as total_quantity,
                       SUM(oi.quantity * oi.unit_price) as total_revenue
                FROM products p
                JOIN order_items oi ON p.product_id = oi.product_id
                JOIN orders o ON oi.order_id = o.order_id
                WHERE o.status = 'completed'
                GROUP BY p.product_id, p.name, p.category
                ORDER BY total_revenue DESC
                LIMIT {MAX_ROWS_DEFAULT}
                """
            else:
                return f"""
                SELECT p.product_id, p.name, p.category, p.unit_price, p.supplier
                FROM products p
                ORDER BY p.name
                LIMIT {MAX_ROWS_DEFAULT}
                """
    
    # Order-related queries
    elif any(word in question for word in ["order", "purchase", "transaction"]):
        if "orders" in tables and "customers" in tables:
            return f"""
            SELECT o.order_id, c.name as customer_name, c.country,
                   o.order_date, o.total_amount, o.status,
                   COUNT(oi.order_item_id) as item_count
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY o.order_id, c.name, c.country, o.order_date, o.total_amount, o.status
            ORDER BY o.order_date DESC
            LIMIT {MAX_ROWS_DEFAULT}
            """
    
    # Revenue/sales queries
    elif any(word in question for word in ["revenue", "sales", "income", "profit"]):
        if "orders" in tables:
            if "month" in question or "monthly" in question:
                return f"""
                SELECT DATE_TRUNC('month', o.order_date) as month,
                       SUM(o.total_amount) as monthly_revenue,
                       COUNT(DISTINCT o.customer_id) as unique_customers,
                       COUNT(o.order_id) as order_count
                FROM orders o
                WHERE o.status = 'completed'
                GROUP BY month
                ORDER BY month DESC
                LIMIT {MAX_ROWS_DEFAULT}
                """
            elif "country" in question:
                return f"""
                SELECT c.country,
                       SUM(o.total_amount) as total_revenue,
                       COUNT(DISTINCT o.customer_id) as customer_count
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                WHERE o.status = 'completed'
                GROUP BY c.country
                ORDER BY total_revenue DESC
                LIMIT {MAX_ROWS_DEFAULT}
                """
            else:
                return f"""
                SELECT SUM(o.total_amount) as total_revenue,
                       COUNT(DISTINCT o.customer_id) as total_customers,
                       AVG(o.total_amount) as avg_order_value
                FROM orders o
                WHERE o.status = 'completed'
                LIMIT {MAX_ROWS_DEFAULT}
                """
    
    # Detailed order items
    elif any(word in question for word in ["detail", "item", "line item", "what bought"]):
        if "order_items" in tables and "orders" in tables and "products" in tables:
            return f"""
            SELECT oi.order_item_id, oi.order_id, p.name as product_name,
                   oi.quantity, oi.unit_price, 
                   (oi.quantity * oi.unit_price) as item_total,
                   c.name as customer_name
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            JOIN products p ON oi.product_id = p.product_id
            JOIN customers c ON o.customer_id = c.customer_id
            ORDER BY oi.order_item_id DESC
            LIMIT {MAX_ROWS_DEFAULT}
            """
    
    # Default: Show recent orders with customer info
    if "orders" in tables and "customers" in tables:
        return f"""
        SELECT o.order_id, c.customer_id, c.name as customer_name, 
               o.order_date, o.total_amount, o.status
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        ORDER BY o.order_date DESC
        LIMIT {MAX_ROWS_DEFAULT}
        """
    
    # Fallback if tables don't exist
    return f"SELECT * FROM information_schema.tables WHERE table_schema = 'public'"

# system_prompt = dedent(f"""
#     You are an expert PostgreSQL SQL generator for a NORMALIZED e-commerce database.
    
#     CRITICAL DATABASE STRUCTURE:
#     ============================
#     1. customers table - Customer information
#     2. products table - Product catalog
#     3. orders table - Order headers (each order belongs to one customer)
#     4. order_items table - Line items (each order has multiple items, each item is one product)

#     RELATIONSHIPS:
#     =============
#     • customers.customer_id → orders.customer_id (One customer has many orders)
#     • orders.order_id → order_items.order_id (One order has many items)
#     • products.product_id → order_items.product_id (One product appears in many order items)
    
#     KEY BUSINESS RULES:
#     ===================
#     1. Revenue = order_items.quantity * order_items.unit_price
#     2. Only count 'completed' orders in revenue calculations (orders.status = 'completed')
#     3. Negative quantity in original data indicates returns/refunds
#     4. Customer information is in customers table, NOT in orders table directly
#     5. Product details are in products table, NOT in order_items table directly
    
#     QUERY GUIDELINES:
#     =================
#     1. You MUST use proper JOINs when querying multiple tables
#     2. Always use table aliases: customers c, orders o, order_items oi, products p
#     3. Prefix column names with table alias when ambiguous
#     4. Use appropriate JOIN types:
#        - INNER JOIN for mandatory relationships (default)
#        - LEFT JOIN when some records might be missing
#     5. When calculating totals, use GROUP BY with appropriate columns
#     6. Always include a LIMIT clause (max {MAX_ROWS_DEFAULT} rows) unless user specifies otherwise
#     7. Use DATE functions for date-based queries: DATE_TRUNC('month', order_date)
#     8. Filter completed orders: WHERE o.status = 'completed'
    
#     COMMON QUERY PATTERNS:
#     ======================
#     1. Customer + Orders: JOIN customers → orders
#     2. Order + Items + Products: JOIN orders → order_items → products
#     3. Customer lifetime value: GROUP BY customer, SUM(order total)
#     4. Product sales analysis: GROUP BY product, SUM(quantity)
#     5. Time-based analysis: GROUP BY DATE_TRUNC('period', order_date)
    
#     IMPORTANT: Return ONLY the SQL query, no explanations, no markdown, no backticks.
#     """).strip()

# If you want this to automatically update when you add new tables/columns, use this function:

# def generate_dynamic_schema_prompt():
#     """Generate a dynamic schema prompt based on actual database"""
#     from ..utils.schema_loader import get_schema_summary
    
#     schema = get_schema_summary()
#     prompt_parts = []
    
#     prompt_parts.append("You are an expert PostgreSQL SQL generator.")
#     prompt_parts.append("\n==============================================")
#     prompt_parts.append("COMPLETE DATABASE SCHEMA")
#     prompt_parts.append("==============================================\n")
    
#     # Add each table with its columns
#     for table_name, columns in schema.get('schema', {}).items():
#         prompt_parts.append(f"{table_name.upper()} TABLE:")
#         prompt_parts.append("-" * 40)
        
#         for col in columns:
#             col_name = col.get('name', '')
#             col_type = col.get('type', '')
#             prompt_parts.append(f"• {col_name} ({col_type})")
        
#         prompt_parts.append("")  # Empty line
    
#     # Add sample data
#     prompt_parts.append("\nSAMPLE DATA (First row of each table):")
#     prompt_parts.append("-" * 40)
    
#     for table_name, samples in schema.get('samples', {}).items():
#         if samples:
#             prompt_parts.append(f"{table_name}: {samples[0]}")
    
#     prompt_parts.append("\n==============================================")
#     prompt_parts.append("QUERY RULES")
#     prompt_parts.append("==============================================")
#     prompt_parts.append("1. Use proper JOINs when querying multiple tables")
#     prompt_parts.append("2. Always use table aliases")
#     prompt_parts.append(f"3. Include LIMIT {MAX_ROWS_DEFAULT} unless specified")
#     prompt_parts.append("4. Return ONLY the SQL query, nothing else")
    
#     return "\n".join(prompt_parts)

# # Then in your generate_sql function:
# system_prompt = generate_dynamic_schema_prompt()