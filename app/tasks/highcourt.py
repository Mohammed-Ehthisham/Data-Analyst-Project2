from __future__ import annotations

from typing import Any, Dict

import math

try:
    import duckdb  # type: ignore
except Exception:
    duckdb = None  # type: ignore


QUERY_COUNT = (
    """
INSTALL httpfs; LOAD httpfs;
INSTALL parquet; LOAD parquet;
SELECT COUNT(*) AS n FROM read_parquet('s3://indian-high-court-judgments/metadata/parquet/year=*/court=*/bench=*/metadata.parquet?s3_region=ap-south-1');
"""
)


def run_highcourt(question_text: str) -> Dict[str, Any]:
    if duckdb is None:
        return {"notes": "duckdb not available"}
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL parquet; LOAD parquet;")
        # Q1: Which high court disposed the most cases 2019-2022?
        q1 = con.execute(
            """
WITH t AS (
  SELECT * FROM read_parquet('s3://indian-high-court-judgments/metadata/parquet/year=2019..2022/court=*/bench=*/metadata.parquet?s3_region=ap-south-1')
)
SELECT court, COUNT(*) AS c
FROM t
GROUP BY court
ORDER BY c DESC
LIMIT 1;
"""
        ).fetchall()
        top_court = q1[0][0] if q1 else "N/A"

        # Q2: regression slope of (decision_date - date_of_registration) by year for court=33_10
        q2 = con.execute(
            """
WITH t AS (
  SELECT * FROM read_parquet('s3://indian-high-court-judgments/metadata/parquet/year=*/court=33_10/bench=*/metadata.parquet?s3_region=ap-south-1')
),
calc AS (
  SELECT strptime(date_of_registration, '%d-%m-%Y') AS dor,
         decision_date::DATE AS dd,
         year
  FROM t
  WHERE date_of_registration IS NOT NULL AND decision_date IS NOT NULL
),
diffs AS (
  SELECT year, datediff('day', dor, dd) AS days
  FROM calc
  WHERE dor IS NOT NULL AND dd IS NOT NULL
)
SELECT CORR(year, days) AS corr, COVAR_SAMP(year, days)/VAR_SAMP(year) AS slope
FROM diffs;
"""
        ).fetchall()
        slope = float(q2[0][1]) if q2 and q2[0][1] is not None else 0.0
        corr = float(q2[0][0]) if q2 and q2[0][0] is not None else 0.0

        # Q3: Generate scatter points for year vs days for court=33_10 limited sample for plotting elsewhere
        pts = con.execute(
            """
WITH t AS (
  SELECT * FROM read_parquet('s3://indian-high-court-judgments/metadata/parquet/year=*/court=33_10/bench=*/metadata.parquet?s3_region=ap-south-1')
),
calc AS (
  SELECT strptime(date_of_registration, '%d-%m-%Y') AS dor,
         decision_date::DATE AS dd,
         year
  FROM t
  WHERE date_of_registration IS NOT NULL AND decision_date IS NOT NULL
),
diffs AS (
  SELECT year, datediff('day', dor, dd) AS days
  FROM calc
  WHERE dor IS NOT NULL AND dd IS NOT NULL
)
SELECT year, days FROM diffs WHERE days IS NOT NULL LIMIT 1000;
"""
        ).fetchall()
        points = [(int(r[0]), float(r[1])) for r in pts]
    except Exception as e:
        try:
            con.close()
        except Exception:
            pass
        return {"notes": f"duckdb error: {e}"}

    try:
        con.close()
    except Exception:
        pass

    return {
        "top_court_2019_2022": top_court,
        "slope_33_10": slope,
        "corr_33_10": corr,
        "points_33_10": points,
    }
