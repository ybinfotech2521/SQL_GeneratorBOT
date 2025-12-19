# backend/app/routes/query.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import os
import time

from ..utils.schema_loader import get_schema_summary
from ..utils.sanitizer import is_safe_select, wrap_with_limit
from ..db.database import SessionLocal
from sqlalchemy import text

from ..llm.sql_generator import generate_sql
from ..llm.answer_formatter import format_answer

router = APIRouter()

class QueryRequest(BaseModel):
    userQuery: str
    includeSchema: bool = True
    maxRows: Optional[int] = None

@router.post("/query")
async def run_query(req: QueryRequest):
    if not req.userQuery or not req.userQuery.strip():
        raise HTTPException(status_code=400, detail="Empty userQuery")

    # 1. load schema optionally
    schema = get_schema_summary() if req.includeSchema else {"schema": {}, "samples": {}}

    # 2. generate SQL via LLM (Grok)
    try:
        sql = await generate_sql(req.userQuery, schema)

        print("\n" + "="*80)
        print("🔍 GENERATED SQL (FULL):")
        print("="*80)
        print(sql)
        print("="*80 + "\n")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL generation error: {e}")

    if not sql:
        raise HTTPException(status_code=500, detail="LLM returned empty SQL")

    # 3. basic safety checks
    if not is_safe_select(sql):
        raise HTTPException(status_code=400, detail="Generated SQL did not pass safety checks (non-SELECT or disallowed keywords).")

    # 4. enforce max rows
    try:
        max_rows = int(req.maxRows) if req.maxRows else int(os.getenv("MAX_QUERY_ROWS", "1000"))
    except Exception:
        max_rows = int(os.getenv("MAX_QUERY_ROWS", "1000"))
    wrapped_sql, params = wrap_with_limit(sql, max_rows)

    # 5. execute SQL
    rows = []
    exec_time_ms = None
    try:
        with SessionLocal() as session:
            start = time.time()
            result = session.execute(text(wrapped_sql), params)
            fetched = result.fetchall()
            exec_time_ms = int((time.time() - start) * 1000)
            rows = [dict(r._mapping) for r in fetched]
    except Exception as e:
        # include original SQL in error only for debugging in dev (avoid in prod)
        raise HTTPException(status_code=500, detail=f"SQL execution error: {e}")

    # 6. format answer via LLM
    try:
        answer = await format_answer(req.userQuery, sql, rows, schema)
    except Exception as e:
        # formatting failure should not hide the data; return rows + SQL
        answer = f"(Answer formatting failed: {e})"

    return {
        "sql": sql,
        "rows": rows,
        "answer": answer,
        "meta": {"row_count": len(rows), "execution_time_ms": exec_time_ms}
    }
