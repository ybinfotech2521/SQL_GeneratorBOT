# backend/app/utils/schema_builder.py
"""
Builds detailed schema descriptions for LLM prompts including relationships.
"""

from sqlalchemy import inspect, text
from ..db.database import engine
import json

def get_detailed_schema():
    """
    Returns a comprehensive schema description including:
    - Table structures with column details
    - Relationships (foreign keys)
    - Sample data
    - Business rules
    - Common query patterns
    """
    
    inspector = inspect(engine)
    schema_info = {
        "tables": {},
        "relationships": [],
        "business_rules": [],
        "common_queries": [],
        "table_counts": {}
    }
    
    with engine.connect() as conn:
        # Get all table names
        tables = inspector.get_table_names(schema='public')
        
        for table in tables:
            # Get row count
            count_result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
            row_count = count_result.fetchone()[0]
            schema_info["table_counts"][table] = row_count
            
            # Get columns with details
            columns = inspector.get_columns(table)
            primary_keys = inspector.get_pk_constraint(table)['constrained_columns']
            foreign_keys = inspector.get_foreign_keys(table)
            
            # Get sample data (first 2 rows)
            try:
                sample_result = conn.execute(text(f"SELECT * FROM {table} LIMIT 2"))
                sample_rows = [dict(row._mapping) for row in sample_result.fetchall()]
            except:
                sample_rows = []
            
            schema_info["tables"][table] = {
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col.get("nullable", True),
                        "primary_key": col["name"] in primary_keys,
                        "default": str(col.get("default", "")) if col.get("default") else None
                    }
                    for col in columns
                ],
                "primary_keys": primary_keys,
                "sample_data": sample_rows,
                "row_count": row_count
            }
            
            # Record relationships
            for fk in foreign_keys:
                relationship_desc = f"One {fk['referred_table'][:-1]} has many {table}" if table.endswith('s') else "One-to-many relationship"
                
                schema_info["relationships"].append({
                    "from_table": table,
                    "from_column": fk["constrained_columns"][0],
                    "to_table": fk["referred_table"],
                    "to_column": fk["referred_columns"][0],
                    "relationship": relationship_desc,
                    "constraint_name": fk.get("name", "unknown")
                })
    
    # Add business rules specific to e-commerce
    schema_info["business_rules"] = [
        "Revenue is calculated as: order_items.quantity * order_items.unit_price",
        "Only orders with status 'completed' should be included in revenue calculations",
        "Customer country determines shipping region and may affect tax rates",
        "Products can appear in multiple order_items with the same product_id",
        "Each order can have multiple order_items (one per product)",
        "Each customer can have multiple orders over time"
    ]
    
    # Add common query patterns for multi-table queries
    schema_info["common_queries"] = [
        "Customer order history: JOIN customers → orders → order_items → products",
        "Monthly revenue analysis: GROUP BY DATE_TRUNC('month', order_date)",
        "Top-selling products: GROUP BY product_id ORDER BY SUM(quantity) DESC",
        "Customer lifetime value: SUM(order total_amount) grouped by customer",
        "Product performance by country: JOIN products → order_items → orders → customers GROUP BY country"
    ]
    
    # Add JOIN templates
    schema_info["join_templates"] = [
        {
            "name": "Full Customer Purchase History",
            "tables": ["customers", "orders", "order_items", "products"],
            "join_conditions": [
                "customers.customer_id = orders.customer_id",
                "orders.order_id = order_items.order_id", 
                "order_items.product_id = products.product_id"
            ]
        },
        {
            "name": "Product Sales Analysis",
            "tables": ["products", "order_items", "orders"],
            "join_conditions": [
                "products.product_id = order_items.product_id",
                "order_items.order_id = orders.order_id"
            ]
        },
        {
            "name": "Customer Geography Analysis",
            "tables": ["customers", "orders"],
            "join_conditions": ["customers.customer_id = orders.customer_id"]
        }
    ]
    
    return schema_info

def format_schema_for_prompt(schema_info):
    """
    Format the schema into a readable string for LLM prompts.
    """
    prompt = "# E-COMMERCE DATABASE SCHEMA\n\n"
    prompt += "## DATABASE OVERVIEW\n"
    prompt += f"Total tables: {len(schema_info['tables'])}\n"
    
    # Table details
    prompt += "\n## TABLES & COLUMNS\n"
    for table_name, table_info in schema_info["tables"].items():
        prompt += f"\n### {table_name.upper()} ({table_info['row_count']} rows)\n"
        
        # Columns
        prompt += "Columns:\n"
        for col in table_info["columns"]:
            flags = []
            if col["primary_key"]:
                flags.append("PK")
            if not col["nullable"]:
                flags.append("NOT NULL")
            if col.get("default"):
                flags.append(f"DEFAULT: {col['default']}")
            
            flag_str = f" ({', '.join(flags)})" if flags else ""
            prompt += f"- {col['name']}: {col['type']}{flag_str}\n"
        
        # Sample data
        if table_info["sample_data"]:
            prompt += f"\nSample row: {table_info['sample_data'][0]}\n"
    
    # Relationships
    prompt += "\n## TABLE RELATIONSHIPS\n"
    if schema_info["relationships"]:
        for rel in schema_info["relationships"]:
            prompt += f"- {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}\n"
            prompt += f"  ({rel['relationship']})\n"
    else:
        prompt += "No foreign key relationships defined.\n"
    
    # Business rules
    prompt += "\n## BUSINESS RULES\n"
    for rule in schema_info["business_rules"]:
        prompt += f"- {rule}\n"
    
    # Common queries
    prompt += "\n## COMMON QUERY PATTERNS\n"
    for query in schema_info["common_queries"]:
        prompt += f"- {query}\n"
    
    # JOIN templates
    prompt += "\n## JOIN TEMPLATES\n"
    for template in schema_info.get("join_templates", []):
        prompt += f"\n### {template['name']}\n"
        prompt += f"Tables: {', '.join(template['tables'])}\n"
        prompt += "JOIN Conditions:\n"
        for condition in template["join_conditions"]:
            prompt += f"- {condition}\n"
    
    # Example queries
    prompt += "\n## EXAMPLE QUERIES\n"
    prompt += """1. Customer with their orders:
   SELECT c.customer_id, c.name, o.order_id, o.order_date, o.total_amount
   FROM customers c
   JOIN orders o ON c.customer_id = o.customer_id
   WHERE o.status = 'completed'
   ORDER BY o.order_date DESC
   LIMIT 10;

2. Order details with product information:
   SELECT o.order_id, p.product_id, p.name as product_name, 
          oi.quantity, oi.unit_price, (oi.quantity * oi.unit_price) as line_total
   FROM orders o
   JOIN order_items oi ON o.order_id = oi.order_id
   JOIN products p ON oi.product_id = p.product_id
   WHERE o.order_id = '536365'
   ORDER BY oi.order_item_id;

3. Monthly revenue by country:
   SELECT c.country, 
          DATE_TRUNC('month', o.order_date) as month,
          SUM(oi.quantity * oi.unit_price) as monthly_revenue,
          COUNT(DISTINCT o.customer_id) as customers
   FROM customers c
   JOIN orders o ON c.customer_id = o.customer_id
   JOIN order_items oi ON o.order_id = oi.order_id
   WHERE o.status = 'completed'
   GROUP BY c.country, DATE_TRUNC('month', o.order_date)
   ORDER BY month DESC, monthly_revenue DESC;"""
    
    return prompt

def get_schema_summary():
    """Backward compatibility with existing code"""
    return get_detailed_schema()