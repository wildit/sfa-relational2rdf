"""
AI inference back-ends.

Two back-ends are supported:
  - openai  : uses the openai Python SDK and works with OpenAI, LM Studio,
              or any other OpenAI-compatible server (Azure OpenAI, etc.)
  - ollama  : uses Ollama's native /api/generate endpoint directly

Both implement the same simple interface: given a plain-text prompt,
return a JSON-parseable string.
"""
from __future__ import annotations

import json
import logging
import re
from abc import ABC, abstractmethod
from typing import TypeVar

import httpx

T = TypeVar("T")
log = logging.getLogger(__name__)


def _extract_json(text: str) -> str:
    """
    Extract the first JSON object or array from *text*.

    LLMs sometimes wrap JSON in markdown code fences; this strips them.
    Mirrors AiUtils.FindJsonContent in the C# project.
    """
    # Strip markdown ```json ... ``` or ``` ... ```
    fence = re.search(r"```(?:json)?\s*([\s\S]+?)```", text)
    if fence:
        return fence.group(1).strip()

    # Try to find a raw JSON object or array
    for start_char, end_char in [('{', '}'), ('[', ']')]:
        start = text.find(start_char)
        if start == -1:
            continue
        depth = 0
        in_string = False
        escape = False
        for i, ch in enumerate(text[start:], start):
            if escape:
                escape = False
                continue
            if ch == '\\' and in_string:
                escape = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if not in_string:
                if ch == start_char:
                    depth += 1
                elif ch == end_char:
                    depth -= 1
                    if depth == 0:
                        return text[start:i + 1]
    return text.strip()


class IInferenceService(ABC):
    @abstractmethod
    def request_json(self, prompt: str, response_type: type[T]) -> T:
        """Send *prompt* and deserialise the response as *response_type*."""


class OpenAIInference(IInferenceService):
    """
    Calls any OpenAI-compatible chat completions endpoint.

    This covers:
      - OpenAI API (https://api.openai.com/v1/)
      - LM Studio  (http://localhost:1234/v1/)
      - Azure OpenAI, Together AI, Groq, …

    The ``openai`` package must be installed.
    """

    def __init__(self, endpoint: str, api_key: str, model: str) -> None:
        from openai import OpenAI  # lazy import

        base_url = endpoint.rstrip("/")
        if not base_url.endswith("/v1"):
            base_url = base_url + "/v1"
        self._client = OpenAI(base_url=base_url, api_key=api_key or "sk-no-key")
        self._model = model
        log.info("OpenAI inference: endpoint=%s model=%s", base_url, model)

    def request_json(self, prompt: str, response_type: type[T]) -> T:
        response = self._client.chat.completions.create(
            model=self._model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.15,
            max_tokens=2048,
        )
        content = response.choices[0].message.content or ""
        json_text = _extract_json(content)
        log.debug("AI response:\nPROMPT: %s\nRESPONSE: %s", prompt[:200], json_text[:400])
        return json.loads(json_text)


class OllamaInference(IInferenceService):
    """
    Calls Ollama's /api/generate endpoint.
    """

    _SYSTEM_PROMPT = (
        "Always format json with \n```json\n```\n so the parser can understand your output"
    )

    def __init__(self, endpoint: str, api_key: str, model: str) -> None:
        base = endpoint.rstrip("/")
        if not base.endswith("/api"):
            base = base + "/api"
        self._base = base
        self._model = model
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        log.info("Ollama inference: endpoint=%s model=%s", base, model)

    def request_json(self, prompt: str, response_type: type[T]) -> T:
        payload = {
            "model": self._model,
            "prompt": prompt,
            "system": self._SYSTEM_PROMPT,
            "stream": False,
            "options": {"temperature": 0.15},
        }
        response = httpx.post(
            f"{self._base}/generate",
            json=payload,
            headers=self._headers,
            timeout=120.0,
        )
        response.raise_for_status()
        data = response.json()
        content = data.get("response", "")
        json_text = _extract_json(content)
        log.debug("Ollama response:\nPROMPT: %s\nRESPONSE: %s", prompt[:200], json_text[:400])
        return json.loads(json_text)


def get_inference_service(
    service: str,
    endpoint: str,
    api_key: str,
    model: str,
) -> IInferenceService:
    """Factory: return the appropriate inference back-end."""
    service_lower = service.lower()
    if service_lower in ("openai", "lmstudio", "lm_studio", "azure"):
        return OpenAIInference(endpoint, api_key, model)
    if service_lower == "ollama":
        return OllamaInference(endpoint, api_key, model)
    raise ValueError(f"Unknown AI service: {service!r}. Choose 'openai' or 'ollama'.")
