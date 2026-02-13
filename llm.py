"""
LLM client — Ollama with JSON mode.
"""

import json
import logging
from typing import Optional, Union

import requests

from config import OLLAMA_TIMEOUT, OLLAMA_URL

log = logging.getLogger(__name__)
OLLAMA_GENERATE_URL = f"{OLLAMA_URL.rstrip('/')}/api/generate"


def run_ollama(prompt: str, model: str = "llama3.1:8b") -> Optional[Union[dict, list]]:
    """
    Run Ollama with JSON mode. Returns parsed JSON (dict or list). Raises on failure.
    """
    payload = {"model": model, "prompt": prompt, "format": "json", "stream": False}

    try:
        response = requests.post(OLLAMA_GENERATE_URL, json=payload, timeout=OLLAMA_TIMEOUT)
        response.raise_for_status()
        out = response.json().get("response", "").strip()

        if out.startswith("```"):
            lines = out.split("\n")
            if lines and lines[0].strip().startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            out = "\n".join(lines)

        data = json.loads(out)
        if not isinstance(data, (dict, list)):
            raise ValueError(f"Expected JSON object or array, got {type(data).__name__}")
        return data
    except requests.exceptions.Timeout:
        log.warning("Ollama timeout")
        raise
    except (requests.RequestException, json.JSONDecodeError, ValueError) as e:
        log.warning(f"Ollama failed: {e}")
        raise
