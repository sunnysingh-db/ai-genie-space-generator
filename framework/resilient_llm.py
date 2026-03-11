"""
Resilient LLM Module
Drop-in replacement for ChatDatabricks that distributes calls across a pool
of model endpoints and retries with fallback on rate-limit / throughput errors.
"""

import random
import time
import warnings
from typing import List, Optional

from langchain_databricks import ChatDatabricks


# Errors that should NEVER be retried (payload issues, bad requests, etc.)
_NON_RETRYABLE_SIGNALS = (
    "request size cannot exceed",
    "payload too large",
    "content length",
    "bad_request",
    "invalid_argument",
)

# Error substrings that indicate a rate-limit / throughput issue worth retrying
_RATE_LIMIT_SIGNALS = (
    "rate limit",
    "rate_limit",
    "throttl",
    "too many requests",
    "429",
    "throughput",
    "capacity",
    "overloaded",
    "resource_exhausted",
    "RESOURCE_EXHAUSTED",
    "temporarily unavailable",
    "server_overloaded",
)


class ResilientLLM:
    """
    A resilient wrapper around ChatDatabricks that distributes LLM calls
    across multiple model endpoints with automatic retry and fallback.

    Usage:
        llm = ResilientLLM(
            model_pool=["databricks-claude-sonnet-4-6", "databricks-claude-opus-4-6"],
            temperature=0,
            timeout=900,
        )
        response = llm.invoke(prompt)   # same return type as ChatDatabricks.invoke()
    """

    def __init__(
        self,
        model_pool: List[str],
        temperature: float = 0,
        timeout: int = 900,
        max_retries: int = None,
        base_backoff: float = 2.0,
        verbose: bool = True,
    ):
        """
        Args:
            model_pool:   List of Databricks Foundation Model endpoint names.
            temperature:  Sampling temperature forwarded to each ChatDatabricks client.
            timeout:      Request timeout in seconds.
            max_retries:  Total retry attempts before giving up.
                          Defaults to len(model_pool) * 2 (each model tried twice).
            base_backoff: Base seconds for exponential backoff (2 → 4 → 8 …).
            verbose:      Print model selection and retry info.
        """
        if not model_pool:
            raise ValueError("model_pool must contain at least one endpoint name")

        self.model_pool = list(model_pool)
        self.temperature = temperature
        self.timeout = timeout
        self.max_retries = max_retries or len(self.model_pool) * 2
        self.base_backoff = base_backoff
        self.verbose = verbose

        # Suppress LangChain deprecation warnings
        warnings.filterwarnings("ignore", category=Warning)

        # Pre-build a ChatDatabricks client per endpoint for connection reuse
        self._clients = {
            endpoint: ChatDatabricks(
                endpoint=endpoint,
                temperature=temperature,
                timeout=timeout,
            )
            for endpoint in self.model_pool
        }

        # Track call stats
        self._call_count = 0
        self._retry_count = 0

    # ── Public API ────────────────────────────────────────────────────────

    def invoke(self, prompt, **kwargs):
        """
        Call the LLM with automatic model rotation and retry on rate limits.

        Args:
            prompt: The prompt string or LangChain message(s) — same as ChatDatabricks.invoke().
            **kwargs: Extra keyword args forwarded to ChatDatabricks.invoke().

        Returns:
            LangChain AIMessage (same return type as ChatDatabricks.invoke()).
        """
        self._call_count += 1

        # Randomly pick the starting model to spread load across endpoints
        shuffled = list(self.model_pool)
        random.shuffle(shuffled)

        last_error = None

        for attempt in range(self.max_retries):
            # Cycle through the shuffled pool
            endpoint = shuffled[attempt % len(shuffled)]
            client = self._clients[endpoint]

            try:
                if self.verbose and attempt > 0:
                    print(f"    🔄 Retry {attempt}/{self.max_retries} → {endpoint}")

                response = client.invoke(prompt, **kwargs)
                return response

            except Exception as e:
                last_error = e
                err_str = str(e).lower()

                if self._is_non_retryable_error(err_str):
                    # Payload/request errors — retrying won't help
                    raise
                elif self._is_rate_limit_error(err_str):
                    self._retry_count += 1
                    backoff = self.base_backoff * (2 ** min(attempt, 4))  # cap at 32s
                    if self.verbose:
                        print(
                            f"    ⚠️  Rate limit on {endpoint} "
                            f"(attempt {attempt + 1}/{self.max_retries}). "
                            f"Backing off {backoff:.0f}s…"
                        )
                    time.sleep(backoff)
                else:
                    # Non-rate-limit error — don't retry, propagate immediately
                    raise

        # Exhausted all retries
        raise RuntimeError(
            f"All {self.max_retries} retry attempts exhausted across model pool "
            f"{self.model_pool}. Last error: {last_error}"
        )

    # ── Convenience ───────────────────────────────────────────────────────

    @property
    def stats(self) -> dict:
        """Return call and retry statistics."""
        return {
            "total_calls": self._call_count,
            "total_retries": self._retry_count,
            "model_pool": self.model_pool,
        }

    def __repr__(self) -> str:
        return (
            f"ResilientLLM(pool={self.model_pool}, "
            f"max_retries={self.max_retries})"
        )

    # ── Internal ──────────────────────────────────────────────────────────

    @staticmethod
    def _is_non_retryable_error(err_str: str) -> bool:
        """Check if the error is a non-retryable client error (e.g. payload too large)."""
        return any(signal in err_str for signal in _NON_RETRYABLE_SIGNALS)

    @staticmethod
    def _is_rate_limit_error(err_str: str) -> bool:
        """Check if the error message indicates a rate-limit / throughput issue."""
        return any(signal in err_str for signal in _RATE_LIMIT_SIGNALS)
