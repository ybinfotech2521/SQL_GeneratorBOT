from typing import Tuple, Dict, Any
import sqlparse
import re

DISALLOWED = [
    "insert", "update", "delete", "drop", "alter", "create",
    "truncate", "grant", "revoke", "copy", ";", "--"
]

def is_safe_select(sql: str) -> bool:
    """
    Basic safety checks for generated SQL.
    Must start with SELECT and must not contain disallowed operations.
    """
    if not sql:
        return False

    sql_low = sql.lower()

    for kw in DISALLOWED:
        if kw in sql_low:
            return False

    # Ensure first token is SELECT
    parsed = sqlparse.parse(sql)
    if not parsed:
        return False

    # regex fallback
    m = re.match(r'^\s*select\b', sql_low, re.I)
    return bool(m)


def wrap_with_limit(sql: str, max_rows: int) -> Tuple[str, Dict[str, Any]]:
    """
    Enforce a max row limit safely.
    Returns:
        (wrapped_sql: str, params: dict)
    """
    # Remove trailing semicolon
    sql = re.sub(r';\s*$', '', sql)

    wrapped_sql = f"SELECT * FROM ({sql}) AS _sub LIMIT :_limit"
    params = {"_limit": max_rows}

    return wrapped_sql, params
