# backend/app/utils/schema_loader.py

from sqlalchemy import text
from ..db.database import engine

def get_schema_summary(limit_sample_rows: int = 3) -> dict:
    """
    Return a compact schema summary:
    {
        "schema": {
            "transactions": [
                {"name": "InvoiceNo", "type": "character varying"},
                ...
            ]
        },
        "samples": {
            "transactions": [
                { "InvoiceNo": "...", "StockCode": "...", ... }
            ]
        }
    }
    """
    schema = {}
    samples = {}

    # ---------------------------
    # 1. Fetch columns for each table
    # ---------------------------
    query = text("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

        for table_name, column_name, data_type in rows:
            schema.setdefault(table_name, []).append({
                "name": column_name,
                "type": data_type
            })

    # ---------------------------
    # 2. Fetch sample rows for each table
    # ---------------------------
    with engine.connect() as conn:
        for table in schema.keys():
            try:
                sample_query = text(f"SELECT * FROM public.{table} LIMIT :limit")
                result = conn.execute(sample_query, {"limit": limit_sample_rows})
                samples[table] = [dict(row._mapping) for row in result.fetchall()]
            except Exception:
                samples[table] = []

    return {
        "schema": schema,
        "samples": samples
    }
