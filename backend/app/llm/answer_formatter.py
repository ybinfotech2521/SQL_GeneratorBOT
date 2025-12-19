# backend/app/llm/answer_formatter.py
"""
Format SQL results into natural language answers with business insights.
Handles multi-table query results intelligently.
"""

from typing import Dict, Any, List
from textwrap import dedent
import json
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.llm.groq_client import call_groq_chat
from app.utils.schema_builder import get_detailed_schema

DEFAULT_MAX_TOKENS = int(os.getenv("LLM_ANSWER_MAX_TOKENS", "512"))
USE_LOCAL_FALLBACK = os.getenv("USE_LOCAL_FALLBACK", "false").lower() == "true"

def detect_query_type(sql: str, question: str) -> str:
    """
    Detect the type of query to format answer appropriately.
    """
    sql_upper = sql.upper()
    question_lower = question.lower()
    
    if "JOIN" in sql_upper:
        # Multi-table query
        if "CUSTOMER" in sql_upper and "PRODUCT" in sql_upper:
            return "customer_product_relationship"
        elif "CUSTOMER" in sql_upper and "ORDER" in sql_upper:
            if "REVENUE" in sql_upper or "SUM" in sql_upper:
                return "customer_revenue"
            else:
                return "customer_orders"
        elif "PRODUCT" in sql_upper and ("SUM" in sql_upper or "COUNT" in sql_upper):
            return "product_sales"
        elif "DATE_TRUNC" in sql_upper or "MONTH" in sql_upper or "YEAR" in sql_upper:
            return "time_series"
        else:
            return "multi_table_general"
    
    # Single table queries
    elif "CUSTOMER" in sql_upper:
        return "customer_list"
    elif "PRODUCT" in sql_upper:
        return "product_list"
    elif "ORDER" in sql_upper:
        if "ITEM" in sql_upper:
            return "order_details"
        else:
            return "order_list"
    elif "REVENUE" in sql_upper or "SUM" in sql_upper or "TOTAL" in sql_upper:
        return "aggregate"
    
    return "general"

def prepare_business_context(query_type: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extract business context from query results.
    """
    context = {
        "has_data": len(rows) > 0,
        "row_count": len(rows),
        "key_metrics": {},
        "trends": [],
        "insights": []
    }
    
    if not rows:
        return context
    
    # Extract sample columns
    sample_row = rows[0]
    columns = list(sample_row.keys())
    context["columns"] = columns
    
    # Extract key metrics based on query type
    try:
        if query_type in ["customer_revenue", "aggregate", "product_sales"]:
            # Look for numeric columns
            numeric_cols = []
            for col in columns:
                if any(keyword in col.lower() for keyword in ['total', 'sum', 'count', 'revenue', 'amount', 'value', 'quantity', 'avg', 'average']):
                    if isinstance(sample_row.get(col), (int, float)):
                        numeric_cols.append(col)
            
            if numeric_cols:
                # Calculate totals
                for col in numeric_cols[:2]:  # Top 2 numeric columns
                    try:
                        total = sum(float(row.get(col, 0) or 0) for row in rows)
                        context["key_metrics"][col] = total
                    except:
                        pass
        
        elif query_type == "time_series":
            # Look for date and value columns
            date_col = None
            value_col = None
            
            for col in columns:
                if any(keyword in col.lower() for keyword in ['date', 'month', 'year', 'time', 'period']):
                    date_col = col
                elif isinstance(sample_row.get(col), (int, float)):
                    value_col = col
            
            if date_col and value_col and len(rows) >= 2:
                # Try to detect trend
                sorted_rows = sorted(rows, key=lambda x: str(x.get(date_col, "")))
                if len(sorted_rows) >= 2:
                    first_val = float(sorted_rows[0].get(value_col, 0) or 0)
                    last_val = float(sorted_rows[-1].get(value_col, 0) or 0)
                    
                    if first_val and last_val and first_val != last_val:
                        trend_pct = ((last_val - first_val) / first_val) * 100
                        direction = "increased" if trend_pct > 0 else "decreased"
                        context["trends"].append(f"Overall trend: {direction} by {abs(trend_pct):.1f}%")
        
        elif query_type in ["customer_product_relationship", "multi_table_general"]:
            # Look for relationship indicators
            if len(rows) > 0:
                # Find categorical columns for grouping insights
                categorical_cols = []
                for col in columns:
                    val = sample_row.get(col)
                    if isinstance(val, str) and len(val) < 50:  # Reasonable string length
                        categorical_cols.append(col)
                
                if categorical_cols:
                    # Find most common values in first categorical column
                    col = categorical_cols[0]
                    value_counts = {}
                    for row in rows[:20]:  # Sample first 20 rows
                        val = row.get(col)
                        if val:
                            value_counts[val] = value_counts.get(val, 0) + 1
                    
                    if value_counts:
                        top_value = max(value_counts.items(), key=lambda x: x[1])
                        context["insights"].append(f"Most common {col}: {top_value[0]} ({top_value[1]} occurrences)")
    
    except Exception as e:
        # Silently fail - context extraction is optional
        pass
    
    return context

async def format_answer(
    user_question: str,
    sql: str,
    rows: List[Dict[str, Any]],
    schema_summary: Dict[str, Any],
    max_tokens: int = DEFAULT_MAX_TOKENS
) -> str:
    """
    Convert SQL results into insightful business answers.
    
    Args:
        user_question: Original user question
        sql: Generated SQL query
        rows: Query results
        schema_summary: Database schema information
        max_tokens: Maximum response length
        
    Returns:
        Natural language answer
    """
    # Quick bypass for testing
    if USE_LOCAL_FALLBACK:
        return generate_local_answer(user_question, sql, rows)
    
    # Detect query type and prepare context
    query_type = detect_query_type(sql, user_question)
    business_context = prepare_business_context(query_type, rows)
    
    # Get schema for additional context
    try:
        detailed_schema = get_detailed_schema()
        table_names = list(detailed_schema.get("tables", {}).keys())
    except:
        table_names = ["customers", "products", "orders", "order_items"]
    
    # Select appropriate system prompt based on query type
    if query_type in ["customer_product_relationship", "multi_table_general", "customer_orders"]:
        system_prompt = dedent("""
        You are a CUSTOMER INSIGHTS ANALYST for an e-commerce company. You translate complex data relationships into specific, actionable customer insights.

        CRITICAL RULES FOR YOUR RESPONSE:
        1. BE SPECIFIC: Mention exact countries, product categories, customer types, or numbers from the data
        2. SHOW RELATIONSHIPS: Explain exactly how entities connect (e.g., "70% of German customers buy Home Decor items")
        3. QUANTIFY: Use percentages, ratios, or comparisons when possible (e.g., "3 times more", "accounts for 40% of")
        4. BUSINESS IMPACT: State the business consequence (e.g., "This suggests we should stock more...", "Indicates a marketing opportunity in...")
        
        BAD EXAMPLE: "Customers show preference for various products."
        GOOD EXAMPLE: "French customers purchased 58% more electronics than UK customers last quarter, suggesting we should prioritize tech inventory for our French warehouse."
        
        RESPONSE STRUCTURE:
        1. SPECIFIC FINDING: "Analysis shows that [exact detail from data]..."
        2. RELATIONSHIP EXPLANATION: "This means that [how entities connect]..."
        3. BUSINESS ACTION: "Consider [specific action] to leverage this insight."
        
        NEVER use vague phrases like: "shows preference", "notable increase", "consistent pattern", "various products"
        ALWAYS reference specific data points from the results provided.
        """).strip()
    
    elif query_type in ["customer_revenue", "product_sales", "aggregate"]:
        system_prompt = dedent("""
        You are a FINANCIAL PERFORMANCE ANALYST. You present precise financial metrics with clear business context.
        
        CRITICAL RULES FOR YOUR RESPONSE:
        1. LEAD WITH NUMBERS: Start with exact totals, averages, or key metrics
        2. ADD CONTEXT: Compare to benchmarks, previous periods, or targets
        3. IDENTIFY DRIVERS: Explain what's driving the numbers (e.g., "Driven by 3 top-selling products...")
        4. CALL OUT EXTREMES: Mention highest/lowest performers specifically
        
        NUMBER FORMATTING:
        - Revenue: "$12,500" not "12500"
        - Percentages: "15.5%" not "0.155"
        - Quantities: "1,200 units" not "1200"
        
        BAD EXAMPLE: "Revenue shows positive trends."
        GOOD EXAMPLE: "Total revenue reached $48,200 in Q4, a 15% increase from Q3. This was driven primarily by 'Wireless Headphones' which accounted for $18,500 (38%) of total sales."
        
        RESPONSE STRUCTURE:
        1. KEY METRIC: "[Exact number] in [category] for [timeframe]"
        2. COMPARISON/CONTEXT: "This represents [change] from [benchmark]"
        3. DRIVER/ACTION: "Largely due to [specific factor]. Consider [action]."
        """).strip()
    
    elif query_type == "time_series":
        system_prompt = dedent("""
        You are a TREND & FORECASTING ANALYST. You identify and explain time-based patterns with precision.
        
        CRITICAL RULES FOR YOUR RESPONSE:
        1. SPECIFY PERIODS: Name exact months, quarters, or years from the data
        2. QUANTIFY CHANGES: "Increased by 40% in March", "Peaked at $25K in December"
        3. IDENTIFY PATTERNS: "Quarterly seasonal pattern", "Consistent month-over-month growth"
        4. PINPOINT EVENTS: "Significant drop in August", "Steady growth from Q1 to Q3"
        
        BAD EXAMPLE: "Revenue shows seasonal trends."
        GOOD EXAMPLE: "Revenue peaked in November at $62,400 (holiday season), dropped 40% in January to $37,400, then recovered steadily through Q1. The November-December period accounted for 35% of annual revenue."
        
        RESPONSE STRUCTURE:
        1. OVERALL TREND: "[Upward/Downward/Flat] trend from [start] to [end]"
        2. KEY PERIODS: "Peak of [value] in [month], low of [value] in [month]"
        3. BUSINESS IMPLICATION: "This suggests [specific operational change] during [period]"
        """).strip()

    elif query_type == "customer_list":
        system_prompt = dedent("""
        You are a CUSTOMER SUCCESS ANALYST. You profile customer segments with actionable insights.
        
        CRITICAL RULES:
        1. NAME TOP PERFORMERS: "Customer 17850 leads with..."
        2. QUANTIFY ACTIVITY: "5 customers account for 60% of..."
        3. SEGMENT CHARACTERISTICS: "The top 10 customers are primarily from..."
        4. RETENTION INSIGHTS: "New vs. returning customer breakdown..."
        
        EXAMPLE: "Customer 17850 from Germany is our top buyer with $8,400 in lifetime value. The top 5 customers (all European) account for 42% of Q4 revenue, suggesting we should develop a European VIP program."
        """).strip()

    elif query_type == "product_list":
        system_prompt = dedent("""
        You are a PRODUCT STRATEGY ANALYST. You analyze product performance with inventory and marketing implications.
        
        CRITICAL RULES:
        1. NAME PRODUCTS: "Red Ceramic Mug sold 1,200 units..."
        2. CATEGORY PERFORMANCE: "Home Decor category leads with..."
        3. PRICE POINT ANALYSIS: "Premium ($50+) products account for..."
        4. STOCK IMPLICATIONS: "Fastest moving items need..."
        
        EXAMPLE: "The 'White Chocolate Reindeer' is our top seller (850 units), but 'Glass Angel Ornaments' generate higher revenue per unit ($28 vs $15). Consider bundling these for holiday promotions."
        """).strip()
    
    else:  # general
        system_prompt = dedent("""
        You are a BUSINESS INTELLIGENCE ANALYST. You provide direct, specific answers to business questions.
        
        CRITICAL RULES:
        1. ANSWER THE QUESTION: Directly address what was asked
        2. BE SPECIFIC: Use numbers, names, categories from the data
        3. AVOID VAGUE LANGUAGE: No "various", "several", "multiple", "consistent"
        4. ONE INSIGHT: Highlight one concrete finding from the data
        
        BAD: "The data shows various trends across different categories."
        GOOD: "The query returned 42 orders totaling $18,400, with 65% from UK customers. Consider increasing marketing budget for UK campaigns."
        
        RESPONSE TEMPLATE:
        "[Direct answer]. Specifically, [key detail from data]. This suggests [one actionable insight]."
        """).strip()

    
    
    # Prepare data summary
    if rows:
        # Limit rows for token efficiency
        display_rows = rows[:5] if len(rows) > 5 else rows
        data_summary = f"Query returned {len(rows)} records."
        
        # Add column context
        if rows and len(rows) > 0:
            columns = list(rows[0].keys())
            data_summary += f"\nColumns: {', '.join(columns[:8])}" + ("..." if len(columns) > 8 else "")
            
            # Add sample data for context
            if display_rows:
                data_summary += f"\n\nSample data (first {len(display_rows)} rows):"
                for i, row in enumerate(display_rows):
                    # Simplify row display
                    simple_row = {}
                    for key, value in row.items():
                        if isinstance(value, (int, float)):
                            simple_row[key] = round(float(value), 2)
                        else:
                            simple_row[key] = str(value)[:30] + "..." if len(str(value)) > 30 else value
                    data_summary += f"\nRow {i+1}: {simple_row}"
    else:
        data_summary = "Query returned no results."
    
    # Add business context if available
    if business_context.get("key_metrics"):
        data_summary += "\n\nKey Metrics Found:"
        for metric, value in business_context["key_metrics"].items():
            if isinstance(value, float):
                data_summary += f"\n• {metric}: {value:,.2f}"
            else:
                data_summary += f"\n• {metric}: {value:,}"
    
    if business_context.get("trends"):
        data_summary += "\n\nTrends Identified:"
        for trend in business_context["trends"]:
            data_summary += f"\n• {trend}"
    
    if business_context.get("insights"):
        data_summary += "\n\nQuick Insights:"
        for insight in business_context["insights"]:
            data_summary += f"\n• {insight}"
    
    # Database context
    db_context = f"""
    DATABASE CONTEXT:
    • Tables available: {', '.join(table_names)}
    • Primary relationships: customers → orders → order_items → products
    • Revenue calculation: quantity × unit_price in order_items table
    """
    
    # Build user prompt
    user_prompt = dedent(f"""
    BUSINESS QUESTION:
    {user_question}
    
    {db_context}
    
    QUERY TYPE DETECTED: {query_type.replace('_', ' ').title()}
    
    EXECUTED SQL (for context only - do not mention in answer):
    {sql}
    
    RESULTS SUMMARY:
    {data_summary}
    
    INSTRUCTIONS FOR YOUR ANSWER:
    1. Directly address the business question
    2. Focus on insights, not data mechanics
    3. Use business terminology
    4. Be concise and professional
    5. Do NOT mention SQL, queries, or technical details
    6. Format: Main insight → Supporting details → Optional follow-up
    7. Length: 2-4 sentences maximum
    
    Provide your business answer now:
    """).strip()
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    try:
        print(f"[Answer Formatter] Formatting {query_type} answer...")
        answer = await call_groq_chat(
            messages=messages,
            max_tokens=max_tokens,
            temperature=0.2  # Slightly creative for better phrasing
        )
        
        if answer and isinstance(answer, str) and answer.strip():
            # Clean up the answer
            answer = answer.strip()
            # Remove any trailing SQL references
            answer = re.sub(r'(?i)(sql|query|select|from|where).*$', '', answer)
            print(f"[Answer Formatter] Generated answer: {answer[:100]}...")
            return answer
        else:
            return generate_local_answer(user_question, sql, rows)
            
    except Exception as e:
        print(f"[Answer Formatter] Groq API error: {e}. Using local answer.")
        return generate_local_answer(user_question, sql, rows)

def generate_local_answer(user_question: str, sql: str, rows: List[Dict[str, Any]]) -> str:
    """
    Generate a local fallback answer when LLM fails.
    Enhanced for multi-table queries.
    """
    if not rows:
        return "No matching records found for your query."
    
    question_lower = user_question.lower()
    row_count = len(rows)
    
    # Try to extract meaningful information
    try:
        sample_row = rows[0]
        columns = list(sample_row.keys())
        
        # Check query type for appropriate response
        sql_upper = sql.upper()
        
        if "JOIN" in sql_upper:
            # Multi-table query response
            if "CUSTOMER" in sql_upper and "PRODUCT" in sql_upper:
                # Customer-product relationship
                if row_count > 0:
                    cust_col = next((c for c in columns if "customer" in c.lower() or "name" in c.lower()), columns[0])
                    prod_col = next((c for c in columns if "product" in c.lower() or "item" in c.lower()), columns[1] if len(columns) > 1 else columns[0])
                    
                    cust_val = sample_row.get(cust_col, "N/A")
                    prod_val = sample_row.get(prod_col, "N/A")
                    
                    return f"Found {row_count} customer-product relationships. For example, {cust_val} purchased {prod_val}. This shows direct purchasing patterns between customers and products."
            
            elif "CUSTOMER" in sql_upper and "ORDER" in sql_upper:
                # Customer orders
                if "SUM" in sql_upper or "TOTAL" in sql_upper:
                    # Customer revenue
                    amount_col = next((c for c in columns if "total" in c.lower() or "amount" in c.lower() or "revenue" in c.lower()), columns[-1])
                    total_amount = sum(float(r.get(amount_col, 0) or 0) for r in rows)
                    
                    return f"Customer order analysis shows {row_count} customer records with total value of ${total_amount:,.2f}. The data reveals customer spending patterns across the business."
                else:
                    # Customer order list
                    return f"Found {row_count} customer orders in the system. Each order represents a purchase transaction with associated customer details and order information."
            
            elif "PRODUCT" in sql_upper and ("SUM" in sql_upper or "COUNT" in sql_upper):
                # Product sales
                qty_col = next((c for c in columns if "quantity" in c.lower() or "count" in c.lower()), None)
                if qty_col:
                    total_qty = sum(float(r.get(qty_col, 0) or 0) for r in rows)
                    return f"Product sales analysis shows {row_count} product records with total quantity sold of {total_qty:,.0f} units. This indicates product performance and demand trends."
        
        # Single table or aggregate responses
        if "CUSTOMER" in sql_upper:
            return f"Customer database contains {row_count} customer records with details including contact information and geographic data."
        
        elif "PRODUCT" in sql_upper:
            return f"Product catalog includes {row_count} products with pricing, category, and supplier information for inventory management."
        
        elif "ORDER" in sql_upper:
            if "ITEM" in sql_upper:
                return f"Order details show {row_count} line items with product quantities, prices, and extended totals for precise order tracking."
            else:
                return f"Order system contains {row_count} orders with customer references, dates, amounts, and status information."
        
        elif "REVENUE" in sql_upper or "SUM" in sql_upper or "TOTAL" in sql_upper:
            # Aggregate query
            for col in columns:
                if isinstance(sample_row.get(col), (int, float)):
                    total = sum(float(r.get(col, 0) or 0) for r in rows)
                    return f"Analysis shows total of {total:,.2f} across {row_count} records. This provides a high-level summary of business performance."
        
        # Default response
        if row_count == 1 and len(columns) == 1:
            # Single value result
            key, value = list(sample_row.items())[0]
            return f"The result is {value}."
        
        return f"Query returned {row_count} records with {len(columns)} data points. This provides insights into the requested business information."
        
    except Exception:
        # Ultra-simple fallback
        return f"Your query returned {row_count} records. Review the data for specific business insights."
    
def detect_query_type_better(sql: str, question: str) -> str:
    """Enhanced query type detection for better answer formatting"""
    sql_upper = sql.upper()
    question_lower = question.lower()
    
    # Time series detection
    if any(word in question_lower for word in ["monthly", "weekly", "daily", "trend", "over time", "by month", "by year"]):
        return "time_series"
    
    # Revenue/sales aggregates
    if any(word in question_lower for word in ["revenue", "sales", "income", "profit", "total", "sum", "average", "aggregate"]):
        if "DATE_TRUNC" in sql_upper or "GROUP BY" in sql_upper:
            return "time_series"
        return "customer_revenue"
    
    # Customer-product relationships
    if ("CUSTOMER" in sql_upper and "PRODUCT" in sql_upper) or \
       ("CUSTOMERS" in sql_upper and "PRODUCTS" in sql_upper):
        return "customer_product_relationship"
    
    # Customer lists
    if "CUSTOMER" in sql_upper and "GROUP BY" in sql_upper and not "PRODUCT" in sql_upper:
        return "customer_list"
    
    # Product lists  
    if "PRODUCT" in sql_upper and "GROUP BY" in sql_upper:
        return "product_list"
    
    # Multi-table general
    if sql_upper.count("JOIN") >= 2:
        return "multi_table_general"
    
    # Customer-orders
    if "CUSTOMER" in sql_upper and "ORDER" in sql_upper and "JOIN" in sql_upper:
        return "customer_orders"
    
    return "general"

# Import regex for cleanup
import re

# if query_type in ["customer_product_relationship", "multi_table_general", "customer_orders"]:
#         system_prompt = dedent("""
#         You are a senior e-commerce business analyst with expertise in customer behavior and sales patterns.
        
#         You are explaining results from a multi-table query that connects different business entities.
        
#         RESPONSE GUIDELINES:
#         ===================
#         1. START with the main business insight that connects the entities
#         2. EXPLAIN relationships clearly (e.g., "Customer X from Country Y purchased Product Z")
#         3. HIGHLIGHT patterns or trends visible in the data
#         4. PROVIDE business implications or actionable insights
#         5. If appropriate, suggest ONE relevant follow-up analysis
#         6. Use plain business language, avoid technical jargon
        
#         FORMAT REQUIREMENTS:
#         ====================
#         • Length: 3-5 concise sentences
#         • Tone: Professional yet approachable
#         • Focus: Business value, not data mechanics
#         • No SQL, no column names, no technical details
        
#         EXAMPLE RESPONSES:
#         =================
#         Good: "German customers show strong preference for electronic gifts, with Customer 17850 being our top buyer in this category. This suggests potential for targeted marketing campaigns in Germany."
#         Bad: "The query returned 15 rows with columns customer_id, country, product_name, quantity."
#         """).strip()
    
#     elif query_type in ["customer_revenue", "product_sales", "aggregate"]:
#         system_prompt = dedent("""
#         You are a financial analyst specializing in e-commerce metrics and KPIs.
        
#         You are presenting financial or sales performance data.
        
#         RESPONSE GUIDELINES:
#         ===================
#         1. START with the key metric or total
#         2. PROVIDE context about what this number means
#         3. COMPARE if relevant (vs previous period, vs average, etc.)
#         4. HIGHLIGHT top performers or outliers if present
#         5. SUGGEST one business implication or action
#         6. Format numbers appropriately (thousands separators, currency symbols)
        
#         FORMAT REQUIREMENTS:
#         ====================
#         • Lead with the most important number
#         • Use business terminology (revenue, margin, conversion, etc.)
#         • Keep it concise: 2-4 sentences
#         • No raw SQL or technical implementation details
#         """).strip()
    
#     elif query_type == "time_series":
#         system_prompt = dedent("""
#         You are a data analyst specializing in trend analysis and forecasting.
        
#         You are explaining time-based patterns in sales or customer behavior.
        
#         RESPONSE GUIDELINES:
#         ===================
#         1. IDENTIFY the overall trend (increasing, decreasing, seasonal)
#         2. HIGHLIGHT peak periods or significant changes
#         3. PROVIDE business context for the trends
#         4. MENTION any cyclical patterns if visible
#         5. SUGGEST one actionable insight based on the trend
        
#         FORMAT REQUIREMENTS:
#         ====================
#         • Clearly state the time period covered
#         • Describe trends in plain language
#         • Reference specific months/periods if significant
#         • 3-4 sentences maximum
#         """).strip()
    
#     else:  # customer_list, product_list, order_list, general
#         system_prompt = dedent("""
#         You are a helpful business analyst assistant.
        
#         Convert data results into clear, actionable business information.
        
#         RESPONSE GUIDELINES:
#         ===================
#         1. ANSWER the user's original question directly
#         2. SUMMARIZE what the data shows
#         3. HIGHLIGHT any notable patterns or exceptions
#         4. KEEP it concise and business-focused
        
#         FORMAT REQUIREMENTS:
#         ====================
#         • Direct answer first
#         • Key findings next
#         • Optional: One follow-up suggestion
#         • 2-3 sentences total
#         """).strip()