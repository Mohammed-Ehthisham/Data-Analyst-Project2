from __future__ import annotations

from typing import Any, Dict

import duckdb  # type: ignore


def run_duckdb_example(question_text: str) -> Dict[str, Any]:
    """Minimal S3 Parquet query scaffold with httpfs/parquet.
    Note: This is a placeholder; real queries will be implemented when needed.
    """
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        con.execute("INSTALL parquet;")
        con.execute("LOAD parquet;")
        # Dummy query (local memory values) as placeholder
        res = con.execute("SELECT 1 as ok").fetchall()
        return {"duckdb": str(res)}
    except Exception as e:
        return {"duckdb_error": str(e)}
    finally:
        try:
            con.close()
        except Exception:
            pass
