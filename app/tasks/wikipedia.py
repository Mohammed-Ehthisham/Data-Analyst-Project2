from __future__ import annotations

from typing import Any, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup


def fetch_table_to_df(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if table is None:
        raise ValueError("No table found")

    rows = []
    headers = []
    # headers
    thead = table.find("thead")
    if thead:
        ths = thead.find_all("th")
        headers = [th.get_text(strip=True) for th in ths]
    if not headers:
        # try first row as header
        first_tr = table.find("tr")
        if first_tr:
            headers = [th.get_text(strip=True) for th in first_tr.find_all(["th", "td"])]

    # body rows
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td"])]
        if cells and (not headers or len(cells) == len(headers)):
            rows.append(cells)

    df = pd.DataFrame(rows, columns=headers if headers else None)
    return df


def run_wikipedia(question_text: str) -> Dict[str, Any]:
    # Simple heuristic: find a wiki URL in the text
    import re

    m = re.search(r"https?://\S+", question_text)
    if not m:
        return {"notes": "no url"}

    url = m.group(0)
    try:
        df = fetch_table_to_df(url)
        return {
            "notes": "wikipedia table fetched",
            "columns": list(df.columns),
            "rows": int(df.shape[0]),
        }
    except Exception as e:
        return {"notes": f"wiki error: {e}"}
