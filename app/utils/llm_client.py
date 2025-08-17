from __future__ import annotations

import os
import json
from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4")
try:
    OPENAI_TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "20"))
except Exception:
    OPENAI_TIMEOUT = 20

_client = None
if OPENAI_API_KEY and OpenAI is not None:
    try:
        _client = OpenAI(api_key=OPENAI_API_KEY)
    except Exception:
        _client = None


def ask_openai(prompt: str, system: str = "You are a helpful data analyst. Respond concisely in JSON.") -> str:
    if _client is None:
        return "[LLM disabled]"
    try:
        resp = _client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            timeout=OPENAI_TIMEOUT,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"[LLM error] {e}"


def ask_openai_json(prompt: str, system: str = "You are a data analyst AI. Return JSON only.") -> Dict[str, Any]:
    """Parse LLM output as JSON dict; fallback to {} on error."""
    try:
        txt = ask_openai(prompt, system)
        return json.loads(txt)
    except Exception:
        return {}
